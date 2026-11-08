package msgconv

import (
	"html"
	"net/url"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type linkSpan struct {
	start, end int
	href       string
	user       id.UserID
}

// Reject every ambiguous connected overlap, not just the later link. Identical
// spans and targets are deduplicated first. Touching endpoints do not overlap.
func unambiguousLinks(links []linkSpan) []linkSpan {
	sort.Slice(links, func(i, j int) bool {
		a, b := links[i], links[j]
		if a.start != b.start {
			return a.start < b.start
		}
		if a.end != b.end {
			return a.end < b.end
		}
		if a.href != b.href {
			return a.href < b.href
		}
		return a.user < b.user
	})
	unique := links[:0]
	for _, l := range links {
		if len(unique) == 0 || unique[len(unique)-1] != l {
			unique = append(unique, l)
		}
	}
	out := unique[:0]
	for i := 0; i < len(unique); {
		j, end := i+1, unique[i].end
		for j < len(unique) && unique[j].start < end {
			if unique[j].end > end {
				end = unique[j].end
			}
			j++
		}
		if j == i+1 {
			out = append(out, unique[i])
		}
		i = j
	}
	return out
}

func safeURL(raw string) bool {
	if raw == "" || strings.IndexFunc(raw, func(r rune) bool { return unicode.IsControl(r) || unicode.IsSpace(r) }) >= 0 {
		return false
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return false
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https":
		return parsed.Host != "" && parsed.User == nil
	case "mailto", "tel":
		return parsed.Opaque != "" && !strings.HasPrefix(parsed.Opaque, "//")
	default:
		return false
	}
}
func validMatrixUser(user id.UserID) bool {
	local, server, err := user.ParseAndValidateRelaxed()
	return err == nil && local != "" && server != "" && strings.IndexFunc(string(user), func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) < 0
}
func payloadAllowed(e sidecar.MessageEntityRecord) bool {
	if e.UserID != nil && e.Kind != "TYPE_MENTION" && e.Kind != "TYPE_BOT_COMMAND" {
		return false
	}
	if e.AgentID != nil && (e.Kind != "TYPE_MENTION" || *e.AgentID <= 0) {
		return false
	}
	if e.GroupID != nil && e.Kind != "TYPE_GROUP_MENTION" {
		return false
	}
	if e.ChatID != nil && e.Kind != "TYPE_THREAD" {
		return false
	}
	return e.Value == nil || e.Kind == "TYPE_PRE" || e.Kind == "TYPE_TEXT_URL"
}

type renderEvent struct {
	at, style, delta int
	link             *linkSpan
}
type wrapper struct{ open, close string }

func render(text string, entities []sidecar.MessageEntityRecord, resolve InlineToMatrix) event.MessageEventContent {
	out := event.MessageEventContent{MsgType: event.MsgText, Body: text, Mentions: &event.Mentions{}}
	// HTML cannot faithfully carry NUL. Keep the exact plain body and no pills.
	if !utf8.ValidString(text) || strings.ContainsRune(text, 0) {
		return out
	}
	boundaries := map[int64]int{0: 0}
	var units int64
	for index, r := range text {
		width := int64(1)
		if r > 0xffff {
			width = 2
		}
		units += width
		boundaries[units] = index + utf8.RuneLen(r)
	}
	budget := markupBudget(text, entities)
	events := make([]renderEvent, 0, len(entities)*2)
	links := make([]linkSpan, 0)
	preLanguage := ""
	languageSet, languageConflict := false, false
	type resolved struct {
		user id.UserID
		href string
		ok   bool
	}
	identities := map[int64]resolved{}
	for _, e := range entities {
		if e.Offset < 0 || e.Length <= 0 || e.Offset > units || e.Length > units-e.Offset || !payloadAllowed(e) {
			continue
		}
		start, ok := boundaries[e.Offset]
		if !ok {
			continue
		}
		end, ok := boundaries[e.Offset+e.Length]
		if !ok {
			continue
		}
		style := -1
		href := ""
		var user id.UserID
		switch e.Kind {
		case "TYPE_BOLD":
			style = 0
		case "TYPE_ITALIC":
			style = 1
		case "TYPE_CODE":
			style = 2
		case "TYPE_PRE":
			style = 3
			lang := ""
			if e.Value != nil {
				lang = *e.Value
			}
			if !languageSet {
				preLanguage = lang
				languageSet = true
			} else if preLanguage != lang {
				languageConflict = true
			}
		case "TYPE_MENTION":
			if e.UserID == nil || *e.UserID <= 0 || resolve == nil {
				continue
			}
			r, seen := identities[*e.UserID]
			if !seen {
				r.user, r.ok = resolve(*e.UserID)
				r.ok = r.ok && validMatrixUser(r.user)
				if r.ok {
					r.href = r.user.URI().MatrixToURL()
				}
				identities[*e.UserID] = r
			}
			if !r.ok {
				continue
			}
			user = r.user
			href = r.href
		case "TYPE_TEXT_URL":
			if e.Value != nil {
				href = *e.Value
			}
		case "TYPE_URL":
			href = text[start:end]
		case "TYPE_EMAIL":
			href = "mailto:" + text[start:end]
		case "TYPE_PHONE_NUMBER":
			href = "tel:" + text[start:end]
		}
		if style >= 0 {
			events = append(events, renderEvent{at: start, style: style, delta: 1}, renderEvent{at: end, style: style, delta: -1})
		} else if href != "" && safeURL(href) {
			links = append(links, linkSpan{start, end, href, user})
		}
	}
	links = unambiguousLinks(links)
	linkOpen := make(map[*linkSpan]string, len(links))
	for i := range links {
		link := &links[i]
		linkOpen[link] = `<a href="` + html.EscapeString(link.href) + `">`
		budget = addBudget(budget, len(link.href), 6)
		events = append(events, renderEvent{at: link.start, style: -1, delta: 1, link: link}, renderEvent{at: link.end, style: -1, delta: -1, link: link})
		out.Mentions.Add(link.user)
	}
	if len(events) == 0 {
		return out
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].at != events[j].at {
			return events[i].at < events[j].at
		}
		return events[i].delta < events[j].delta
	})
	preOpen := `<pre><code>`
	if preLanguage != "" && !languageConflict {
		preOpen = `<pre><code class="language-` + html.EscapeString(preLanguage) + `">`
	}
	var htmlOut strings.Builder
	counts := [4]int{}
	var active *linkSpan
	previous := []wrapper{}
	setWrappers := func(next []wrapper) {
		common := 0
		for common < len(previous) && common < len(next) && previous[common] == next[common] {
			common++
		}
		for i := len(previous) - 1; i >= common; i-- {
			htmlOut.WriteString(previous[i].close)
		}
		for i := common; i < len(next); i++ {
			htmlOut.WriteString(next[i].open)
		}
		previous = next
	}
	position := 0
	for i := 0; i < len(events); {
		at := events[i].at
		if at > position {
			writeEscaped(&htmlOut, text[position:at])
		}
		j := i
		for j < len(events) && events[j].at == at {
			e := events[j]
			if e.style >= 0 {
				counts[e.style] += e.delta
			} else if e.delta < 0 {
				active = nil
			} else {
				active = e.link
			}
			j++
		}
		next := make([]wrapper, 0, 5)
		if active != nil {
			next = append(next, wrapper{linkOpen[active], `</a>`})
		}
		if counts[3] > 0 {
			next = append(next, wrapper{preOpen, `</code></pre>`})
		}
		if counts[0] > 0 {
			next = append(next, wrapper{`<strong>`, `</strong>`})
		}
		if counts[1] > 0 {
			next = append(next, wrapper{`<em>`, `</em>`})
		}
		if counts[2] > 0 && counts[3] == 0 {
			next = append(next, wrapper{`<code>`, `</code>`})
		}
		setWrappers(next)
		if htmlOut.Len() > budget {
			return event.MessageEventContent{MsgType: event.MsgText, Body: text, Mentions: &event.Mentions{}}
		}
		position = at
		i = j
	}
	if position < len(text) {
		writeEscaped(&htmlOut, text[position:])
	}
	setWrappers(nil)
	if htmlOut.Len() > budget {
		return event.MessageEventContent{MsgType: event.MsgText, Body: text, Mentions: &event.Mentions{}}
	}
	out.Format = event.FormatHTML
	out.FormattedBody = htmlOut.String()
	return out
}
func writeEscaped(out *strings.Builder, text string) {
	escaped := html.EscapeString(text)
	escaped = strings.ReplaceAll(escaped, "\r", "&#13;")
	escaped = strings.ReplaceAll(escaped, "\n", "<br>")
	out.WriteString(escaped)
}

// Six bytes covers HTML escaping each input byte. 256 bytes per entity covers
// reopening the fixed wrapper stack at both endpoints. Payload bytes are
// charged once; pathological crossings cannot multiply them without fallback.
func markupBudget(text string, entities []sidecar.MessageEntityRecord) int {
	budget := addBudget(0, len(text), 6)
	budget = addBudget(budget, len(entities), 256)
	for _, entity := range entities {
		if entity.Value != nil {
			budget = addBudget(budget, len(*entity.Value), 6)
		}
	}
	return budget
}
func addBudget(budget, size, factor int) int {
	maximum := int(^uint(0) >> 1)
	if size > (maximum-budget)/factor {
		return maximum
	}
	return budget + size*factor
}
