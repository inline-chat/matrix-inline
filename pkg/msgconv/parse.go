package msgconv

import (
	"io"
	"strings"
	"unicode/utf8"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"golang.org/x/net/html"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type parseFrame struct {
	tag      string
	previous int
	start    int64
	entity   sidecar.MessageEntityRecord
	href     string
}

func blockTag(tag string) bool {
	switch tag {
	case "p", "div", "li", "ul", "ol", "blockquote", "pre", "h1", "h2", "h3", "h4", "h5", "h6", "hr", "table", "tr":
		return true
	}
	return false
}
func inactiveTag(tag string) bool {
	switch tag {
	case "script", "style", "iframe", "object", "embed", "svg", "math", "template", "mx-reply":
		return true
	}
	return false
}

// HTML ignores closing slashes on non-void containers. Foreign self-closing
// SVG/Math and void embed end immediately, including inside another blocker.
func opensInactiveTag(kind html.TokenType, tag string) bool {
	return tag != "embed" && (kind == html.StartTagToken ||
		(kind == html.SelfClosingTagToken && tag != "svg" && tag != "math"))
}

func attr(token html.Token, name string) string {
	for _, a := range token.Attr {
		if a.Key == name {
			return a.Val
		}
	}
	return ""
}

func parse(content *event.MessageEventContent, resolve MatrixToInline) (string, []sidecar.MessageEntityRecord) {
	if content.Format != event.FormatHTML || content.FormattedBody == "" || !formattedUTF8(content) {
		return content.Body, nil
	}
	tokenizer := html.NewTokenizer(strings.NewReader(content.FormattedBody))
	var text strings.Builder
	var position int64
	pendingBreak := false
	lastNewline := false
	write := func(value string) {
		if value == "" {
			return
		}
		if pendingBreak && text.Len() > 0 && !lastNewline {
			text.WriteByte('\n')
			position++
		}
		pendingBreak = false
		text.WriteString(value)
		for _, r := range value {
			position++
			if r > 0xffff {
				position++
			}
		}
		lastNewline = strings.HasSuffix(value, "\n")
	}
	flushBreak := func() {
		if pendingBreak && text.Len() > 0 && !lastNewline {
			text.WriteByte('\n')
			position++
			lastNewline = true
		}
		pendingBreak = false
	}
	stack := []parseFrame{}
	latest := map[string]int{}
	entities := []sidecar.MessageEntityRecord{}
	links := []linkSpan{}
	type resolved struct {
		userID int64
		ok     bool
	}
	identities := map[id.UserID]resolved{}
	closeTo := func(index int) {
		for i := len(stack) - 1; i >= index; i-- {
			frame := stack[i]
			if frame.previous < 0 {
				delete(latest, frame.tag)
			} else {
				latest[frame.tag] = frame.previous
			}
			length := position - frame.start
			if length <= 0 {
				continue
			}
			if frame.tag == "a" {
				links = append(links, linkSpan{start: int(frame.start), end: int(position), href: frame.href})
				continue
			}
			if frame.entity.Kind != "" {
				e := frame.entity
				e.Offset = frame.start
				e.Length = length
				entities = append(entities, e)
			}
		}
		stack = stack[:index]
	}
	blocked := ""
	blockedDepth := 0
	for {
		kind := tokenizer.Next()
		if kind == html.ErrorToken {
			if tokenizer.Err() != io.EOF {
				return content.Body, nil
			}
			break
		}
		token := tokenizer.Token()
		if blocked != "" {
			if token.Data == blocked && opensInactiveTag(kind, token.Data) {
				blockedDepth++
			}
			if kind == html.EndTagToken && token.Data == blocked {
				blockedDepth--
				if blockedDepth == 0 {
					blocked = ""
				}
			}
			continue
		}
		if kind == html.TextToken {
			write(strings.ReplaceAll(token.Data, "\x00", "�"))
			continue
		}
		if kind != html.StartTagToken && kind != html.SelfClosingTagToken && kind != html.EndTagToken {
			continue
		}
		tag := token.Data
		if inactiveTag(tag) {
			if opensInactiveTag(kind, tag) {
				blocked = tag
				blockedDepth = 1
			}
			continue
		}
		if kind == html.EndTagToken {
			if index, ok := latest[tag]; ok {
				closeTo(index)
			}
			if blockTag(tag) {
				pendingBreak = text.Len() > 0
			}
			continue
		}
		if tag == "br" {
			write("\n")
			continue
		}
		if blockTag(tag) {
			pendingBreak = text.Len() > 0
		}
		frame := parseFrame{tag: tag, previous: -1}
		switch tag {
		case "b", "strong":
			frame.entity.Kind = "TYPE_BOLD"
		case "i", "em":
			frame.entity.Kind = "TYPE_ITALIC"
		case "code":
			if pre, ok := latest["pre"]; ok {
				for _, class := range strings.Fields(attr(token, "class")) {
					if strings.HasPrefix(class, "language-") {
						language := strings.TrimPrefix(class, "language-")
						stack[pre].entity.Value = &language
						break
					}
				}
				continue
			}
			frame.entity.Kind = "TYPE_CODE"
		case "pre":
			frame.entity.Kind = "TYPE_PRE"
		case "a":
			frame.href = attr(token, "href")
		default:
			continue
		}
		flushBreak()
		frame.start = position
		if previous, ok := latest[tag]; ok {
			frame.previous = previous
		}
		latest[tag] = len(stack)
		stack = append(stack, frame)
		if kind == html.SelfClosingTagToken {
			closeTo(len(stack) - 1)
		}
	}
	closeTo(0)
	for _, link := range unambiguousLinks(links) {
		e := sidecar.MessageEntityRecord{Kind: "TYPE_TEXT_URL", Offset: int64(link.start), Length: int64(link.end - link.start), Value: &link.href}
		if uri, err := id.ParseMatrixURIOrMatrixToURL(link.href); err == nil && uri.UserID() != "" && validMatrixUser(uri.UserID()) && resolve != nil {
			user := uri.UserID()
			r, seen := identities[user]
			if !seen {
				r.userID, r.ok = resolve(user)
				r.ok = r.ok && r.userID > 0
				identities[user] = r
			}
			if r.ok {
				e.Kind = "TYPE_MENTION"
				e.Value = nil
				e.UserID = &r.userID
			}
		}
		if e.Kind == "TYPE_MENTION" || safeURL(link.href) {
			entities = append(entities, e)
		}
	}
	return text.String(), entities
}

// Avoid accepting an invalid UTF-8 formatted buffer as an alternate source of
// identity labels. The ordinary body remains the host's exact fallback.
func formattedUTF8(content *event.MessageEventContent) bool {
	return utf8.ValidString(content.FormattedBody)
}
