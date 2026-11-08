package msgconv

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"golang.org/x/net/html"
	"hegel.dev/go/hegel"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func ptr[T any](v T) *T { return &v }
func ent(kind string, offset, length int64) sidecar.MessageEntityRecord {
	return sidecar.MessageEntityRecord{Kind: kind, Offset: offset, Length: length}
}
func htmlText(t *testing.T, text string) string {
	t.Helper()
	root, err := html.Parse(strings.NewReader("<div>" + text + "</div>"))
	if err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	stack := []*html.Node{root}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if n.Type == html.TextNode {
			out.WriteString(n.Data)
		}
		if n.Type == html.ElementNode && n.Data == "br" {
			out.WriteByte('\n')
		}
		for c := n.LastChild; c != nil; c = c.PrevSibling {
			stack = append(stack, c)
		}
	}
	return out.String()
}
func allowInline(n int64) (id.UserID, bool) { return id.UserID(fmt.Sprintf("@u%d:test", n)), n > 0 }
func findEntity(entities []sidecar.MessageEntityRecord, kind string) *sidecar.MessageEntityRecord {
	for _, e := range entities {
		if e.Kind == kind {
			return &e
		}
	}
	return nil
}

// Dropping entities, using byte offsets or marking all names as mentions breaks this.
func TestRenderExactUnicodeAndScopedMentions(t *testing.T) {
	text := "😀 A<&\"\nA"
	mention := ent("TYPE_MENTION", 3, 4)
	mention.UserID = ptr(int64(9))
	got := ToMatrix(text, []sidecar.MessageEntityRecord{ent("TYPE_BOLD", 0, 2), mention, ent("TYPE_GROUP_MENTION", 8, 1)}, allowInline)
	if got.Body != text || got.Format != event.FormatHTML || htmlText(t, got.FormattedBody) != text {
		t.Fatalf("render: %+v", got)
	}
	if !strings.Contains(got.FormattedBody, "<strong>") || !strings.Contains(got.FormattedBody, "&lt;") {
		t.Fatalf("unescaped or unformatted: %q", got.FormattedBody)
	}
	if got.Mentions == nil || got.Mentions.Room || !reflect.DeepEqual(got.Mentions.UserIDs, []id.UserID{"@u9:test"}) {
		t.Fatalf("wrong mentions: %+v", got.Mentions)
	}
	denied := ToMatrix(text, []sidecar.MessageEntityRecord{mention}, func(int64) (id.UserID, bool) { return "@unrelated:test", false })
	if denied.Mentions == nil || len(denied.Mentions.UserIDs) != 0 || denied.Mentions.Room {
		t.Fatalf("denied resolution mentioned: %+v", denied.Mentions)
	}
}

// Malformed and ambiguous identity spans must never turn a different label into a pill.
func TestRenderRejectsMalformedAndCrossingMentions(t *testing.T) {
	for _, r := range [][2]int64{{1, 1}, {-1, 2}, {0, 0}, {0, math.MaxInt64}, {math.MaxInt64, 2}, {0, -1}} {
		e := ent("TYPE_MENTION", r[0], r[1])
		e.UserID = ptr(int64(9))
		got := ToMatrix("😀abc", []sidecar.MessageEntityRecord{e}, allowInline)
		if got.Mentions == nil || len(got.Mentions.UserIDs) != 0 || strings.Contains(got.FormattedBody, "<a ") {
			t.Fatalf("invalid span %v mentioned: %+v", r, got)
		}
	}
	a, b := ent("TYPE_MENTION", 0, 3), ent("TYPE_MENTION", 2, 3)
	a.UserID = ptr(int64(1))
	b.UserID = ptr(int64(2))
	got := ToMatrix("abcde", []sidecar.MessageEntityRecord{a, b, ent("TYPE_BOLD", 0, 3), ent("TYPE_ITALIC", 2, 3)}, allowInline)
	if got.Mentions == nil || len(got.Mentions.UserIDs) != 0 || got.Mentions.Room {
		t.Fatalf("ambiguous spans: %+v", got)
	}
	if htmlText(t, got.FormattedBody) != "abcde" {
		t.Fatal(got.FormattedBody)
	}
}

// Dropping formatting or using byte/rune counts instead of UTF-16 changes exact spans.
func TestParseFormattingAndTypedPills(t *testing.T) {
	input := &event.MessageEventContent{Body: "fallback", Format: event.FormatHTML, FormattedBody: `😀 <strong>A</strong> <em>é</em> <a href="https://matrix.to/#/@verified:test">same</a> <a href="https://matrix.to/#/@other:test">same</a>`}
	text, entities := FromMatrix(input, func(mx id.UserID) (int64, bool) { return 42, mx == "@verified:test" })
	if text != "😀 A é same same" {
		t.Fatalf("text %q", text)
	}
	want := []sidecar.MessageEntityRecord{ent("TYPE_BOLD", 3, 1), ent("TYPE_ITALIC", 5, 2)}
	for _, w := range want {
		got := findEntity(entities, w.Kind)
		if got == nil || *got != w {
			t.Fatalf("%s: %+v", w.Kind, entities)
		}
	}
	mention := findEntity(entities, "TYPE_MENTION")
	if mention == nil || mention.Offset != 8 || mention.Length != 4 || mention.UserID == nil || *mention.UserID != 42 {
		t.Fatalf("mention: %+v", entities)
	}
	count := 0
	for _, e := range entities {
		if e.Kind == "TYPE_MENTION" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("identity confusion: %+v", entities)
	}
}

func TestParseSafeLinksPreAndReadableUnsupportedBlocks(t *testing.T) {
	input := &event.MessageEventContent{Body: "fallback", Format: event.FormatHTML, FormattedBody: `<p><a href="javascript:alert(1)">bad</a><script>hidden()</script><a href="https://safe.test/?a=1&amp;b=&quot;x&quot;" onclick="evil()">safe</a></p><ul><li>one</li><li><s>two</s></li></ul><blockquote>quote</blockquote><pre><code class="language-go">x &lt; y</code></pre>`}
	text, entities := FromMatrix(input, nil)
	if text != "badsafe\none\ntwo\nquote\nx < y" {
		t.Fatalf("text %q", text)
	}
	link := findEntity(entities, "TYPE_TEXT_URL")
	if link == nil || link.Value == nil || *link.Value != `https://safe.test/?a=1&b="x"` || link.Offset != 3 || link.Length != 4 {
		t.Fatalf("link: %+v", entities)
	}
	pre := findEntity(entities, "TYPE_PRE")
	if pre == nil || pre.Value == nil || *pre.Value != "go" || pre.Length != 5 {
		t.Fatalf("pre: %+v", entities)
	}
	if findEntity(entities, "TYPE_CODE") != nil {
		t.Fatalf("duplicate pre/code: %+v", entities)
	}
}

func TestPlainContentIsNeverMarkdownOrHTMLParsed(t *testing.T) {
	for _, format := range []event.Format{"", "unknown"} {
		input := &event.MessageEventContent{Body: "**literal** <b>x</b> 😀", Format: format, FormattedBody: "<b>ignored</b>"}
		text, entities := FromMatrix(input, nil)
		if text != input.Body || len(entities) != 0 {
			t.Fatalf("plain: %q %+v", text, entities)
		}
	}
}

// Independent oracle enumerates UTF-16 encoded rune boundaries, including invalid full signed ranges.
func TestSignedRangesMentionExactlyTheOriginalSpan(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		raw := hegel.Draw(ht, hegel.Text())
		runes := []rune(raw)
		if len(runes) > 80 {
			runes = runes[:80]
		}
		text := string(runes)
		off := hegel.Draw(ht, hegel.Integers[int64](math.MinInt64, math.MaxInt64))
		length := hegel.Draw(ht, hegel.Integers[int64](math.MinInt64, math.MaxInt64))
		e := ent("TYPE_MENTION", off, length)
		e.UserID = ptr(int64(1))
		got := ToMatrix(text, []sidecar.MessageEntityRecord{e}, allowInline)
		bounds := map[int64]bool{0: true}
		var pos int64
		for _, r := range runes {
			pos += int64(len(utf16.Encode([]rune{r})))
			bounds[pos] = true
		}
		valid := off >= 0 && length > 0 && off <= math.MaxInt64-length && bounds[off] && bounds[off+length] && !strings.ContainsRune(text, 0)
		if got.Body != text {
			ht.Fatalf("plain body changed")
		}
		if got.Mentions == nil || got.Mentions.Room || (len(got.Mentions.UserIDs) == 1) != valid {
			ht.Fatalf("range %d+%d valid=%v mentions=%+v", off, length, valid, got.Mentions)
		}
	}, hegel.WithTestCases(3000), hegel.WithDatabase(t.TempDir()))
}

func TestParseMatrixURIPillsAndAmbiguousAnchors(t *testing.T) {
	input := &event.MessageEventContent{Format: event.FormatHTML, FormattedBody: `<a href="matrix:u/verified:test">😀</a>!`}
	text, entities := FromMatrix(input, func(mx id.UserID) (int64, bool) { return 42, mx == "@verified:test" })
	mention := findEntity(entities, "TYPE_MENTION")
	if text != "😀!" || mention == nil || mention.Offset != 0 || mention.Length != 2 || mention.UserID == nil || *mention.UserID != 42 {
		t.Fatalf("matrix URI pill: %q %+v", text, entities)
	}
	input.FormattedBody = `<a href="https://matrix.to/#/@verified:test">A<a href="https://matrix.to/#/@other:test">B</a>C</a>`
	text, entities = FromMatrix(input, func(mx id.UserID) (int64, bool) {
		if mx == "@verified:test" {
			return 42, true
		}
		return 99, true
	})
	if text != "ABC" || findEntity(entities, "TYPE_MENTION") != nil {
		t.Fatalf("ambiguous nested pills: %q %+v", text, entities)
	}
}

func TestEverySupportedSourceKindPreservesUnicodeSpans(t *testing.T) {
	kinds := []string{"TYPE_BOLD", "TYPE_ITALIC", "TYPE_CODE", "TYPE_PRE", "TYPE_MENTION", "TYPE_TEXT_URL", "TYPE_URL", "TYPE_EMAIL", "TYPE_PHONE_NUMBER", "TYPE_GROUP_MENTION", "TYPE_THREAD", "TYPE_USERNAME_MENTION", "TYPE_BOT_COMMAND", "TYPE_UNDERLINE", "TYPE_STRIKETHROUGH", "TYPE_HIGHLIGHT", "TYPE_MATH", "TYPE_BLOCKQUOTE", "TYPE_THREAD_TITLE", "TYPE_UNSPECIFIED", "future"}
	hegel.Test(t, func(ht *hegel.T) {
		raw := hegel.Draw(ht, hegel.Text())
		runes := []rune(raw)
		if len(runes) > 80 {
			runes = runes[:80]
		}
		runes = append(runes, []rune("A😀é<&>\r\n")...)
		a := hegel.Draw(ht, hegel.Integers[uint64](0, math.MaxUint64))
		b := hegel.Draw(ht, hegel.Integers[uint64](0, math.MaxUint64))
		start := int(a % uint64(len(runes)))
		end := start + 1 + int(b%uint64(len(runes)-start))
		kind := hegel.Draw(ht, hegel.SampledFrom(kinds))
		off := int64(len(utf16.Encode(runes[:start])))
		length := int64(len(utf16.Encode(runes[start:end])))
		e := ent(kind, off, length)
		switch kind {
		case "TYPE_MENTION":
			e.UserID = ptr(int64(7))
		case "TYPE_GROUP_MENTION":
			e.GroupID = ptr(int64(2))
		case "TYPE_THREAD":
			e.ChatID = ptr(int64(3))
		case "TYPE_PRE":
			e.Value = ptr("go")
		case "TYPE_TEXT_URL":
			e.Value = ptr(`https://safe.test/?q="<&`)
		}
		text := string(runes)
		got := ToMatrix(text, []sidecar.MessageEntityRecord{e}, allowInline)
		if got.Body != text {
			ht.Fatal("body changed")
		}
		if got.FormattedBody != "" && htmlText(t, got.FormattedBody) != text {
			ht.Fatalf("HTML text changed %q => %q", text, got.FormattedBody)
		}
		expectMention := kind == "TYPE_MENTION" && !strings.ContainsRune(text, 0)
		if got.Mentions == nil || got.Mentions.Room || (len(got.Mentions.UserIDs) == 1) != expectMention {
			ht.Fatalf("kind=%s %+v", kind, got.Mentions)
		}
		if expectMention {
			root, err := html.Parse(strings.NewReader(got.FormattedBody))
			if err != nil {
				ht.Fatal(err)
			}
			nodes := []*html.Node{root}
			label := ""
			for len(nodes) > 0 {
				n := nodes[len(nodes)-1]
				nodes = nodes[:len(nodes)-1]
				if n.Type == html.ElementNode && n.Data == "a" {
					var s strings.Builder
					children := []*html.Node{n}
					for len(children) > 0 {
						c := children[len(children)-1]
						children = children[:len(children)-1]
						if c.Type == html.TextNode {
							s.WriteString(c.Data)
						}
						if c.Type == html.ElementNode && c.Data == "br" {
							s.WriteByte('\n')
						}
						for ch := c.LastChild; ch != nil; ch = ch.PrevSibling {
							children = append(children, ch)
						}
					}
					label += s.String()
				}
				for c := n.LastChild; c != nil; c = c.PrevSibling {
					nodes = append(nodes, c)
				}
			}
			if label != string(runes[start:end]) {
				ht.Fatalf("wrong mention label %q expected %q", label, string(runes[start:end]))
			}
		}
	}, hegel.WithTestCases(3000), hegel.WithDatabase(t.TempDir()))
}

func TestParserEscapingAndExactFormattingRanges(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		raw := hegel.Draw(ht, hegel.Text())
		runes := []rune(raw)
		if len(runes) > 80 {
			runes = runes[:80]
		}
		value := string(runes)
		// This oracle follows the HTML parser's specified NUL/CR normalization.
		expected := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(value, "\x00", "�"), "\r\n", "\n"), "\r", "\n")
		input := &event.MessageEventContent{Body: "fallback", Format: event.FormatHTML, FormattedBody: "😀<strong>" + strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(value) + "</strong>!"}
		text, entities := FromMatrix(input, nil)
		if text != "😀"+expected+"!" {
			ht.Fatalf("decoded text: %q want %q", text, "😀"+expected+"!")
		}
		bold := findEntity(entities, "TYPE_BOLD")
		wantLength := int64(len(utf16.Encode([]rune(expected))))
		if wantLength == 0 {
			if bold != nil {
				ht.Fatal("empty span retained")
			}
		} else if bold == nil || bold.Offset != 2 || bold.Length != wantLength {
			ht.Fatalf("exact span: %+v expected len%d", entities, wantLength)
		}
	}, hegel.WithTestCases(3000), hegel.WithDatabase(t.TempDir()))
}

func TestScaleIterativeParsingAndRendering(t *testing.T) {
	const count = 100000
	text := strings.Repeat("x", count)
	entities := make([]sidecar.MessageEntityRecord, count)
	for i := range entities {
		kind := "TYPE_BOLD"
		if i%2 == 1 {
			kind = "TYPE_ITALIC"
		}
		entities[i] = ent(kind, int64(i), 1)
	}
	got := ToMatrix(text, entities, nil)
	if got.Body != text || htmlText(t, got.FormattedBody) != text {
		t.Fatal("large render lost content")
	}
	parsed, native := FromMatrix(&event.MessageEventContent{Format: event.FormatHTML, FormattedBody: strings.Repeat("<b>", count) + "😀" + strings.Repeat("</b>", count)}, nil)
	if parsed != "😀" || len(native) != count {
		t.Fatalf("deep parsing lost content/entities %q %d", parsed, len(native))
	}
	for _, e := range native {
		if e.Kind != "TYPE_BOLD" || e.Offset != 0 || e.Length != 2 {
			t.Fatalf("deep entity %+v", e)
		}
	}
}

// Crossings can otherwise repeat a long pre-language payload for every link.
func TestRenderCrossingPayloadExpansionIsBounded(t *testing.T) {
	text := strings.Repeat("xy", 2500)
	language := strings.Repeat("l", 4096)
	pre := ent("TYPE_PRE", 0, int64(len(text)))
	pre.Value = &language
	entities := []sidecar.MessageEntityRecord{pre}
	for i := 0; i < len(text); i += 2 {
		e := ent("TYPE_TEXT_URL", int64(i), 1)
		e.Value = ptr("https://safe.test")
		entities = append(entities, e)
	}
	got := ToMatrix(text, entities, nil)
	// A generous linear envelope catches multiplied payloads. Falling back to
	// the exact body is allowed when markup would exceed the documented budget.
	envelope := 64 * (len(text) + len(language) + len(entities)*len("https://safe.test"))
	if got.Body != text || len(got.FormattedBody) > envelope {
		t.Fatalf("unbounded expansion: text=%d payload=%d entities=%d HTML=%d", len(text), len(language), len(entities), len(got.FormattedBody))
	}
}

func TestUnsafeSchemesActiveContentAndInvalidResolversNeverMention(t *testing.T) {
	for _, href := range []string{`javascript:alert(1)`, `JaVaScRiPt:alert(1)`, `data:text/html,bad`, `file:///etc/passwd`, `//evil.test`, ` https://safe.test`, `https://safe.test/` + "\n" + `bad`} {
		e := ent("TYPE_TEXT_URL", 0, 1)
		e.Value = &href
		got := ToMatrix("X", []sidecar.MessageEntityRecord{e}, allowInline)
		if strings.Contains(got.FormattedBody, "<a ") || len(got.Mentions.UserIDs) != 0 || got.Mentions.Room {
			t.Fatalf("unsafe URL %q: %+v", href, got)
		}
	}
	for _, htmlSource := range []string{`<script><a href="https://matrix.to/#/@x:test">secret</a></script>ok`, `<svg><a href="https://matrix.to/#/@x:test">secret</a></svg>ok`, `<mx-reply>secret</mx-reply>ok`} {
		calls := 0
		body, entities := FromMatrix(&event.MessageEventContent{Format: event.FormatHTML, FormattedBody: htmlSource}, func(id.UserID) (int64, bool) { calls++; return 1, true })
		if body != "ok" || len(entities) != 0 || calls != 0 {
			t.Fatalf("active/fallback content: %q %+v %d", body, entities, calls)
		}
	}
	for _, user := range []id.UserID{"", "not-an-id", "@x:bad server", id.UserID("@" + strings.Repeat("x", 300) + ":test")} {
		e := ent("TYPE_MENTION", 0, 1)
		e.UserID = ptr(int64(1))
		got := ToMatrix("X", []sidecar.MessageEntityRecord{e}, func(int64) (id.UserID, bool) { return user, true })
		if len(got.Mentions.UserIDs) != 0 {
			t.Fatalf("invalid resolved user %q accepted", user)
		}
	}
}

// A renderer that simply drops crossing styles would pass text-only checks.
func TestCrossingStylesMatchIndependentPerUTF16Coverage(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		raw := hegel.Draw(ht, hegel.Text())
		runes := []rune(raw)
		if len(runes) > 80 {
			runes = runes[:80]
		}
		runes = append(runes, []rune("😀éxy")...)
		text := string(runes)
		var units []int64
		units = append(units, 0)
		for _, r := range runes {
			units = append(units, units[len(units)-1]+int64(len(utf16.Encode([]rune{r}))))
		}
		var entities []sidecar.MessageEntityRecord
		want := make([]uint8, units[len(units)-1])
		for i, kind := range []string{"TYPE_BOLD", "TYPE_ITALIC"} {
			a := hegel.Draw(ht, hegel.Integers[uint64](0, math.MaxUint64))
			b := hegel.Draw(ht, hegel.Integers[uint64](0, math.MaxUint64))
			start := int(a % uint64(len(runes)))
			end := start + 1 + int(b%uint64(len(runes)-start))
			entities = append(entities, ent(kind, units[start], units[end]-units[start]))
			for p := units[start]; p < units[end]; p++ {
				want[p] |= 1 << i
			}
		}
		got := ToMatrix(text, entities, nil)
		if got.Body != text {
			ht.Fatal("body changed")
		}
		if strings.ContainsRune(text, 0) {
			if got.FormattedBody != "" {
				ht.Fatal("NUL fallback missing")
			}
			return
		}
		root, err := html.Parse(strings.NewReader("<div>" + got.FormattedBody + "</div>"))
		if err != nil {
			ht.Fatal(err)
		}
		type item struct {
			node *html.Node
			mask uint8
		}
		stack := []item{{root, 0}}
		var masks []uint8
		for len(stack) > 0 {
			x := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if x.node.Type == html.ElementNode {
				switch x.node.Data {
				case "strong":
					x.mask |= 1
				case "em":
					x.mask |= 2
				case "br":
					masks = append(masks, x.mask)
				}
			}
			if x.node.Type == html.TextNode {
				for _, r := range x.node.Data {
					for range utf16.Encode([]rune{r}) {
						masks = append(masks, x.mask)
					}
				}
			}
			for child := x.node.LastChild; child != nil; child = child.PrevSibling {
				stack = append(stack, item{child, x.mask})
			}
		}
		if !reflect.DeepEqual(masks, want) {
			ht.Fatalf("style coverage got%v want%v HTML%q", masks, want, got.FormattedBody)
		}
	}, hegel.WithTestCases(3000), hegel.WithDatabase(t.TempDir()))
}

func TestParserEveryNativeFormattingKindAndIdentityDecision(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		tag := hegel.Draw(ht, hegel.SampledFrom([]string{"b", "strong", "i", "em", "code", "pre", "a"}))
		allow := hegel.Draw(ht, hegel.SampledFrom([]bool{false, true}))
		userID := hegel.Draw(ht, hegel.Integers[int64](1, math.MaxInt64))
		value := "A😀é<&"
		open := "<" + tag + ">"
		close := "</" + tag + ">"
		kind := map[string]string{"b": "TYPE_BOLD", "strong": "TYPE_BOLD", "i": "TYPE_ITALIC", "em": "TYPE_ITALIC", "code": "TYPE_CODE", "pre": "TYPE_PRE", "a": "TYPE_TEXT_URL"}[tag]
		if tag == "a" {
			open = `<a href="https://matrix.to/#/@verified:test">`
			if allow {
				kind = "TYPE_MENTION"
			}
		}
		text, entities := FromMatrix(&event.MessageEventContent{Format: event.FormatHTML, FormattedBody: open + "A😀é&lt;&amp;" + close}, func(mx id.UserID) (int64, bool) { return userID, allow && mx == "@verified:test" })
		if text != value || len(entities) != 1 {
			ht.Fatalf("text/entities %q %+v", text, entities)
		}
		e := entities[0]
		if e.Kind != kind || e.Offset != 0 || e.Length != int64(len(utf16.Encode([]rune(value)))) {
			ht.Fatalf("native entity %+v", e)
		}
		if kind == "TYPE_MENTION" && (e.UserID == nil || *e.UserID != userID || e.Value != nil) {
			ht.Fatalf("wrong target %+v", e)
		}
	}, hegel.WithTestCases(3000), hegel.WithDatabase(t.TempDir()))
}

func TestNilInvalidUTF8AndMalformedIdentityUseExactFallback(t *testing.T) {
	text, entities := FromMatrix(nil, nil)
	if text != "" || len(entities) != 0 {
		t.Fatalf("nil: %q %+v", text, entities)
	}
	invalid := string([]byte{'A', 0xff, 'B'})
	mention := ent("TYPE_MENTION", 0, 1)
	mention.UserID = ptr(int64(1))
	calls := 0
	got := ToMatrix(invalid, []sidecar.MessageEntityRecord{mention}, func(int64) (id.UserID, bool) { calls++; return "@x:test", true })
	if got.Body != invalid || got.FormattedBody != "" || len(got.Mentions.UserIDs) != 0 || calls != 0 {
		t.Fatal("invalid UTF8 rendered or resolved")
	}
	input := &event.MessageEventContent{Body: "exact **fallback**", Format: event.FormatHTML, FormattedBody: invalid}
	text, entities = FromMatrix(input, func(id.UserID) (int64, bool) { calls++; return 1, true })
	if text != input.Body || len(entities) != 0 || calls != 0 {
		t.Fatalf("bad HTML fallback: %q %+v", text, entities)
	}
	mention.GroupID = ptr(int64(5))
	got = ToMatrix("X", []sidecar.MessageEntityRecord{mention}, func(int64) (id.UserID, bool) { calls++; return "@x:test", true })
	if len(got.Mentions.UserIDs) != 0 || calls != 0 {
		t.Fatal("conflicting identity payload resolved")
	}
	for _, userID := range []int64{0, -1, math.MinInt64} {
		text, entities = FromMatrix(&event.MessageEventContent{Format: event.FormatHTML, FormattedBody: `<a href="https://matrix.to/#/@x:test">X</a>`}, func(id.UserID) (int64, bool) { return userID, true })
		if text != "X" || findEntity(entities, "TYPE_MENTION") != nil {
			t.Fatalf("invalid native target %d: %q %+v", userID, text, entities)
		}
	}
}

func TestInactiveVoidEmbedPreservesSuffix(t *testing.T) {
	for _, markup := range []string{`A<embed src="https://unused.test">B`, `A<embed src="https://unused.test"/>B`} {
		calls := 0
		body, entities := FromMatrix(&event.MessageEventContent{Body: "AB", Format: event.FormatHTML, FormattedBody: markup}, func(id.UserID) (int64, bool) { calls++; return 7, true })
		if body != "AB" || len(entities) != 0 || calls != 0 {
			t.Fatalf("void element lost suffix or resolved identity: %q %+v calls%d", body, entities, calls)
		}
	}
}

func TestInactiveSelfClosingContainersDoNotLeakContent(t *testing.T) {
	for _, tag := range []string{"script", "style", "iframe", "object", "template", "mx-reply"} {
		for _, closed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/closed=%v", tag, closed), func(t *testing.T) {
				markup := "A<" + tag + "/>hidden<a href=\"https://matrix.to/#/@secret:test\">secret</a>"
				want := "A"
				if closed {
					markup += "</" + tag + ">B"
					want = "AB"
				}
				calls := 0
				got, entities := FromMatrix(&event.MessageEventContent{Body: "fallback", Format: event.FormatHTML, FormattedBody: markup}, func(id.UserID) (int64, bool) { calls++; return 7, true })
				if got != want || len(entities) != 0 || calls != 0 {
					t.Fatalf("inactive content leaked or resolved: %q want%q entities%+v calls%d", got, want, entities, calls)
				}
			})
		}
	}
	for _, tag := range []string{"svg", "math"} {
		got, entities := FromMatrix(&event.MessageEventContent{Body: "fallback", Format: event.FormatHTML, FormattedBody: "A<" + tag + "/>B"}, nil)
		if got != "AB" || len(entities) != 0 {
			t.Fatalf("closed foreign element consumed suffix: %q %+v", got, entities)
		}
	}
}

func TestInactiveSelfClosingVisibleBoundaryProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		tag := hegel.Draw(ht, hegel.SampledFrom([]string{"script", "style", "iframe", "object", "template", "mx-reply"}))
		closed := hegel.Draw(ht, hegel.Booleans())
		parts := make([]string, 2)
		for i := range parts {
			runes := []rune(hegel.Draw(ht, hegel.Text()))
			if len(runes) > 80 {
				runes = runes[:80]
			}
			parts[i] = string(runes)
		}
		normalize := strings.NewReplacer("\r\n", "\n", "\r", "\n", "\x00", "�")
		escape := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
		markup := escape.Replace(parts[0]) + "A<" + tag + "/>hidden<a href=\"https://matrix.to/#/@secret:test\">secret</a>"
		want := normalize.Replace(parts[0]) + "A"
		if closed {
			markup += "</" + tag + ">B" + escape.Replace(parts[1])
			want += "B" + normalize.Replace(parts[1])
		}
		calls := 0
		got, entities := FromMatrix(&event.MessageEventContent{Body: "fallback", Format: event.FormatHTML, FormattedBody: markup}, func(id.UserID) (int64, bool) { calls++; return 7, true })
		if got != want || len(entities) != 0 || calls != 0 {
			ht.Fatalf("visible boundary changed: %q want%q entities%+v calls%d", got, want, entities, calls)
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}

func TestInactiveNestedSelfClosingContainersRemainBlockedProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		tag := hegel.Draw(ht, hegel.SampledFrom([]string{"template", "object", "mx-reply"}))
		depth := 1 + int(hegel.Draw(ht, hegel.Integers[uint64](0, math.MaxUint64))%16)
		var markup strings.Builder
		markup.WriteString("A")
		for range depth {
			markup.WriteString("<" + tag)
			if hegel.Draw(ht, hegel.Booleans()) {
				markup.WriteString("/")
			}
			markup.WriteString(">")
		}
		for range depth {
			markup.WriteString(`<a href="https://matrix.to/#/@secret:test">hidden</a></` + tag + ">")
		}
		markup.WriteString("B")
		calls := 0
		got, entities := FromMatrix(&event.MessageEventContent{Body: "fallback", Format: event.FormatHTML, FormattedBody: markup.String()}, func(id.UserID) (int64, bool) { calls++; return 7, true })
		if got != "AB" || len(entities) != 0 || calls != 0 {
			ht.Fatalf("nested inactive content leaked: %q entities%+v calls%d HTML%q", got, entities, calls, markup.String())
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}

func TestInactiveVoidEmbedSuffixProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		runes := []rune(hegel.Draw(ht, hegel.Text()))
		if len(runes) > 80 {
			runes = runes[:80]
		}
		suffix := string(runes) + "B"
		expected := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(suffix, "\x00", "�"), "\r\n", "\n"), "\r", "\n")
		encoded := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(suffix)
		calls := 0
		got, entities := FromMatrix(&event.MessageEventContent{Body: "fallback", Format: event.FormatHTML, FormattedBody: `A<embed src="https://unused.test" href="https://matrix.to/#/@x:test">` + encoded}, func(id.UserID) (int64, bool) { calls++; return 7, true })
		if got != "A"+expected || len(entities) != 0 || calls != 0 {
			ht.Fatalf("suffix lost: %q want %q, entities%+v resolver%d", got, "A"+expected, entities, calls)
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}

// This oracle requires exact positive decoration coverage; empty HTML cannot
// pass by preserving only the plain body. It does not call converter helpers.
func TestCodePreAndSafeLinkHaveExactPositiveCoverage(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		runes := []rune(hegel.Draw(ht, hegel.Text()))
		if len(runes) > 80 {
			runes = runes[:80]
		}
		runes = append(runes, []rune("A😀éxy")...)
		text := string(runes)
		a := hegel.Draw(ht, hegel.Integers[uint64](0, math.MaxUint64))
		b := hegel.Draw(ht, hegel.Integers[uint64](0, math.MaxUint64))
		start := int(a % uint64(len(runes)))
		end := start + 1 + int(b%uint64(len(runes)-start))
		kind := hegel.Draw(ht, hegel.SampledFrom([]string{"TYPE_CODE", "TYPE_PRE", "TYPE_TEXT_URL"}))
		off := len(utf16.Encode(runes[:start]))
		length := len(utf16.Encode(runes[start:end]))
		entity := ent(kind, int64(off), int64(length))
		tag := "code"
		href := "https://safe.test/?q=%22%3C%26"
		if kind == "TYPE_PRE" {
			tag = "pre"
			entity.Value = ptr("go")
		}
		if kind == "TYPE_TEXT_URL" {
			tag = "a"
			entity.Value = &href
		}
		got := ToMatrix(text, []sidecar.MessageEntityRecord{entity}, nil)
		if got.Body != text {
			ht.Fatal("plain body changed")
		}
		if strings.ContainsRune(text, 0) {
			if got.FormattedBody != "" {
				ht.Fatal("NUL must keep plain fallback")
			}
			return
		}
		if got.Format != event.FormatHTML || got.FormattedBody == "" {
			ht.Fatalf("supported%s decoration dropped", kind)
		}
		if htmlText(t, got.FormattedBody) != text {
			ht.Fatal("decorated text changed")
		}
		root, err := html.Parse(strings.NewReader("<div>" + got.FormattedBody + "</div>"))
		if err != nil {
			ht.Fatal(err)
		}
		type nodeState struct {
			node   *html.Node
			active bool
		}
		stack := []nodeState{{node: root}}
		var actual []bool
		for len(stack) > 0 {
			x := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if x.node.Type == html.ElementNode && x.node.Data == tag {
				x.active = true
				if tag == "a" {
					found := false
					for _, attribute := range x.node.Attr {
						if attribute.Key == "href" {
							found = attribute.Val == href
						}
					}
					if !found {
						ht.Fatal("safe link destination changed")
					}
				}
			}
			if x.node.Type == html.TextNode {
				for _, r := range x.node.Data {
					for range utf16.Encode([]rune{r}) {
						actual = append(actual, x.active)
					}
				}
			}
			if x.node.Type == html.ElementNode && x.node.Data == "br" {
				actual = append(actual, x.active)
			}
			for child := x.node.LastChild; child != nil; child = child.PrevSibling {
				stack = append(stack, nodeState{child, x.active})
			}
		}
		want := make([]bool, len(utf16.Encode(runes)))
		for i := off; i < off+length; i++ {
			want[i] = true
		}
		if !reflect.DeepEqual(actual, want) {
			ht.Fatalf("%s coverage differs: got%v want%v", kind, actual, want)
		}
	}, hegel.WithTestCases(3000), hegel.WithDatabase(t.TempDir()))
}

func TestInactiveVoidEmbedKeepsSuffixEntityUTF16Offset(t *testing.T) {
	body, entities := FromMatrix(&event.MessageEventContent{Body: "A😀B", Format: event.FormatHTML, FormattedBody: `A<embed src="https://unused.test"><b>😀B</b>`}, nil)
	if body != "A😀B" || len(entities) != 1 || entities[0].Kind != "TYPE_BOLD" || entities[0].Offset != 1 || entities[0].Length != 3 {
		t.Fatalf("void tag shifted suffix entity: %q %+v", body, entities)
	}
}
