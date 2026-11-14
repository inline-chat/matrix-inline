package connector

import (
	"context"
	"fmt"
	"html"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	xhtml "golang.org/x/net/html"
	"golang.org/x/net/html/atom"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// Native entity offsets count UTF-16 code units, not bytes or Unicode scalars.
// A -1 boundary is the middle of a surrogate pair and must never be sliced.
func utf16Boundaries(text string) []int {
	bounds := []int{0}
	for offset, r := range text {
		if r > 0xffff {
			bounds = append(bounds, -1)
		}
		bounds = append(bounds, offset+utf8.RuneLen(r))
	}
	return bounds
}
func utf16Length(text string) int { return len(utf16Boundaries(text)) - 1 }

func safeEntityURL(value string) string {
	if strings.ContainsAny(value, "\x00\r\n") {
		return ""
	}
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil {
		return ""
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https":
		if parsed.Host == "" || parsed.User != nil {
			return ""
		}
	case "mailto", "tel":
		if parsed.Opaque == "" && parsed.Path == "" {
			return ""
		}
	default:
		return ""
	}
	return parsed.String()
}

type nativeMentionResolver func(int64) id.UserID

func inlineMentionResolver(portal *bridgev2.Portal) nativeMentionResolver {
	return func(userID int64) id.UserID {
		if userID <= 0 || portal == nil || portal.Bridge == nil || portal.Bridge.Matrix == nil {
			return ""
		}
		login := portal.Bridge.GetCachedUserLoginByID(portal.Receiver)
		if login == nil {
			return ""
		}
		client, ok := login.Client.(*InlineClient)
		if !ok {
			return ""
		}
		if _, known := client.cachedUser(userID); !known {
			return ""
		}
		intent := portal.Bridge.Matrix.GhostIntent(makeUserID(strconv.FormatInt(userID, 10)))
		if intent == nil {
			return ""
		}
		return intent.GetMXID()
	}
}

func (ic *InlineClient) matrixMentionResolver(mxid id.UserID) *int64 {
	if ic.UserLogin == nil || ic.UserLogin.Bridge == nil || ic.UserLogin.Bridge.Matrix == nil {
		return nil
	}
	nativeID, ok := ic.UserLogin.Bridge.Matrix.ParseGhostMXID(mxid)
	if !ok {
		return nil
	}
	userID, err := strconv.ParseInt(string(nativeID), 10, 64)
	if err != nil || userID <= 0 {
		return nil
	}
	if _, known := ic.cachedUser(userID); !known {
		return nil
	}
	return &userID
}

// matrixTextToInline walks the Matrix HTML tree directly. Markdown is not an
// intermediate representation, so literal punctuation and nested entities survive.
func matrixTextToInline(content *event.MessageEventContent, caption bool, resolve func(id.UserID) *int64) (string, []sidecar.MessageEntityRecord, error) {
	entities := make([]sidecar.MessageEntityRecord, 0)
	if content == nil {
		return "", entities, fmt.Errorf("missing Matrix message content")
	}
	text := content.Body
	if caption {
		text = content.GetCaption()
	}
	if content.Format != event.FormatHTML || content.FormattedBody == "" || (caption && text == "") {
		if !utf8.ValidString(text) || utf16Length(text) > 5000 {
			return "", nil, fmt.Errorf("Inline text must be valid UTF-8 and at most 5000 UTF-16 units")
		}
		return text, entities, nil
	}
	if len(content.FormattedBody) > 1<<20 {
		return "", nil, fmt.Errorf("Matrix formatted message is too large")
	}
	nodes, err := xhtml.ParseFragment(strings.NewReader(content.FormattedBody), &xhtml.Node{Type: xhtml.ElementNode, Data: "div", DataAtom: atom.Div})
	if err != nil {
		return "", nil, fmt.Errorf("parse Matrix formatted text: %w", err)
	}
	var body strings.Builder
	units := int64(0)
	write := func(value string) { body.WriteString(value); units += int64(utf16Length(value)) }
	newline := func() {
		if body.Len() > 0 && !strings.HasSuffix(body.String(), "\n") {
			write("\n")
		}
	}
	var walk func(*xhtml.Node, int, bool) error
	walk = func(node *xhtml.Node, depth int, inPre bool) error {
		if depth > 128 {
			return fmt.Errorf("Matrix formatted text nesting is too deep")
		}
		if node.Type == xhtml.TextNode {
			write(node.Data)
			return nil
		}
		if node.Type != xhtml.ElementNode {
			return nil
		}
		tag := strings.ToLower(node.Data)
		switch tag {
		case "script", "style", "mx-reply":
			return nil
		case "br":
			write("\n")
			return nil
		}
		block := tag == "p" || tag == "div" || tag == "blockquote" || tag == "li" || tag == "pre"
		if block {
			newline()
		}
		start := units
		entity := sidecar.MessageEntityRecord{Offset: start}
		attr := func(name string) string {
			for _, a := range node.Attr {
				if a.Key == name {
					return a.Val
				}
			}
			return ""
		}
		switch tag {
		case "b", "strong":
			entity.Kind = "TYPE_BOLD"
		case "i", "em":
			entity.Kind = "TYPE_ITALIC"
		case "u":
			entity.Kind = "TYPE_UNDERLINE"
		case "s", "del", "strike":
			entity.Kind = "TYPE_STRIKETHROUGH"
		case "code":
			if !inPre {
				entity.Kind = "TYPE_CODE"
			}
		case "pre":
			entity.Kind = "TYPE_PRE"
			if child := node.FirstChild; child != nil && child.Type == xhtml.ElementNode && child.Data == "code" {
				for _, a := range child.Attr {
					if a.Key == "class" {
						for _, cls := range strings.Fields(a.Val) {
							if strings.HasPrefix(cls, "language-") {
								value := strings.TrimPrefix(cls, "language-")
								entity.Value = &value
							}
						}
					}
				}
			}
		case "a":
			href := safeEntityURL(attr("href"))
			if href != "" {
				entity.Kind = "TYPE_TEXT_URL"
				entity.Value = &href
				parsed, _ := url.Parse(href)
				if parsed.Host == "matrix.to" && strings.HasPrefix(parsed.Fragment, "/@") && resolve != nil {
					target := strings.SplitN(strings.TrimPrefix(parsed.Fragment, "/"), "?", 2)[0]
					if userID := resolve(id.UserID(target)); userID != nil {
						entity.Kind = "TYPE_MENTION"
						entity.UserID = userID
						entity.Value = nil
					}
				}
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			if err := walk(child, depth+1, inPre || tag == "pre"); err != nil {
				return err
			}
		}
		entity.Length = units - start
		if entity.Kind != "" && entity.Length > 0 {
			entities = append(entities, entity)
			if len(entities) > 2048 {
				return fmt.Errorf("Matrix formatted text has too many entities")
			}
		}
		if block && node.NextSibling != nil {
			newline()
		}
		if units > 5000 {
			return fmt.Errorf("Inline text exceeds 5000 UTF-16 units")
		}
		return nil
	}
	for _, node := range nodes {
		if err := walk(node, 0, false); err != nil {
			return "", nil, err
		}
	}
	text = body.String()
	if !utf8.ValidString(text) || units > 5000 {
		return "", nil, fmt.Errorf("invalid Inline message text")
	}
	sort.SliceStable(entities, func(i, j int) bool {
		if entities[i].Offset == entities[j].Offset {
			return entities[i].Length > entities[j].Length
		}
		return entities[i].Offset < entities[j].Offset
	})
	return text, entities, nil
}

type formattedSpan struct {
	start, end  int
	open, close string
	mention     id.UserID
}

func applyInlineEntities(content *event.MessageEventContent, text string, entities []sidecar.MessageEntityRecord, resolve nativeMentionResolver) {
	// An explicit empty mention set disables legacy @room/display-name inference.
	content.Mentions = &event.Mentions{}
	if text == "" || !utf8.ValidString(text) || len(text) > 1<<20 || len(entities) > 2048 {
		return
	}
	bounds := utf16Boundaries(text)
	spans := make([]formattedSpan, 0, len(entities))
	points := map[int]bool{0: true, len(text): true}
	for _, entity := range entities {
		if entity.Offset < 0 || entity.Length <= 0 || entity.Offset >= int64(len(bounds)) || entity.Length > int64(len(bounds))-1-entity.Offset {
			continue
		}
		start, end := bounds[int(entity.Offset)], bounds[int(entity.Offset+entity.Length)]
		if start < 0 || end < 0 {
			continue
		}
		span := formattedSpan{start: start, end: end}
		switch entity.Kind {
		case "TYPE_BOLD":
			span.open, span.close = "<strong>", "</strong>"
		case "TYPE_ITALIC":
			span.open, span.close = "<em>", "</em>"
		case "TYPE_UNDERLINE":
			span.open, span.close = "<u>", "</u>"
		case "TYPE_STRIKETHROUGH":
			span.open, span.close = "<del>", "</del>"
		case "TYPE_CODE":
			span.open, span.close = "<code>", "</code>"
		case "TYPE_PRE":
			span.open, span.close = "<pre><code>", "</code></pre>"
			if entity.Value != nil && *entity.Value != "" {
				span.open = "<pre><code class=\"language-" + html.EscapeString(*entity.Value) + "\">"
			}
		case "TYPE_URL", "TYPE_TEXT_URL", "TYPE_EMAIL", "TYPE_PHONE", "TYPE_PHONE_NUMBER":
			value := text[start:end]
			if entity.Kind == "TYPE_TEXT_URL" {
				if entity.Value == nil {
					continue
				}
				value = *entity.Value
			}
			if entity.Kind == "TYPE_EMAIL" {
				value = "mailto:" + value
			}
			if entity.Kind == "TYPE_PHONE" || entity.Kind == "TYPE_PHONE_NUMBER" {
				value = "tel:" + value
			}
			href := safeEntityURL(value)
			if href == "" {
				continue
			}
			span.open, span.close = "<a href=\""+html.EscapeString(href)+"\">", "</a>"
		case "TYPE_MENTION":
			if entity.UserID == nil || resolve == nil {
				continue
			}
			span.mention = resolve(*entity.UserID)
			if span.mention == "" {
				continue
			}
			span.open, span.close = "<a href=\"https://matrix.to/#/"+html.EscapeString(url.PathEscape(string(span.mention)))+"\">", "</a>"
			content.Mentions.Add(span.mention)
		default:
			continue
		}
		spans = append(spans, span)
		points[start] = true
		points[end] = true
	}
	if len(spans) == 0 {
		return
	}
	sort.SliceStable(spans, func(i, j int) bool {
		if spans[i].start == spans[j].start {
			if spans[i].end == spans[j].end {
				return spans[i].open < spans[j].open
			}
			return spans[i].end > spans[j].end
		}
		return spans[i].start < spans[j].start
	})
	positions := make([]int, 0, len(points))
	for point := range points {
		positions = append(positions, point)
	}
	sort.Ints(positions)
	var output strings.Builder
	// Keep the common nesting prefix. Only crossing spans need to close and
	// reopen, keeping normal nested formatting compact and linear in text size.
	previous := make([]int, 0)
	for i := 0; i < len(positions)-1; i++ {
		start, end := positions[i], positions[i+1]
		active := make([]int, 0)
		link := false
		for index, span := range spans {
			if span.start <= start && span.end >= end {
				if span.close == "</a>" {
					if link {
						continue
					}
					link = true
				}
				active = append(active, index)
			}
		}
		common := 0
		for common < len(previous) && common < len(active) && previous[common] == active[common] {
			common++
		}
		for j := len(previous) - 1; j >= common; j-- {
			output.WriteString(spans[previous[j]].close)
		}
		for j := common; j < len(active); j++ {
			output.WriteString(spans[active[j]].open)
		}
		output.WriteString(strings.ReplaceAll(html.EscapeString(text[start:end]), "\n", "<br>"))
		if output.Len() > 1<<20 {
			content.Mentions = &event.Mentions{}
			return
		}
		previous = active
	}
	for j := len(previous) - 1; j >= 0; j-- {
		output.WriteString(spans[previous[j]].close)
	}

	content.Format = event.FormatHTML
	content.FormattedBody = output.String()
}

func applyInlineRecordEntities(ctx context.Context, portal *bridgev2.Portal, converted *bridgev2.ConvertedMessage, data sidecar.MessageRecord) {
	if converted == nil {
		return
	}
	for _, part := range converted.Parts {
		if part == nil || part.Content == nil {
			continue
		}
		text := data.Content.Text
		if data.Content.Type == "media" {
			if part.Content.URL == "" && part.Content.File == nil {
				part.Content.Mentions = &event.Mentions{}
				continue
			}
			text = ""
			if data.Content.Caption != nil {
				text = *data.Content.Caption
			}
		}
		applyInlineEntities(part.Content, text, data.Metadata.Entities, inlineMentionResolver(portal))
	}
}
