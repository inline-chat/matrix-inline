package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"hegel.dev/go/hegel"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/crypto/attachment"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func TestMediaFidelityGenericAudioUsesDocumentFallback(t *testing.T) {
	for _, voice := range []bool{false, true} {
		c := &event.MessageEventContent{MsgType: event.MsgAudio, Info: &event.FileInfo{MimeType: "audio/ogg", Duration: 1250}}
		if voice {
			c.MSC3245Voice = &event.MSC3245Voice{}
		}
		if got := sidecarUploadKindForMatrix(c); got != "document" {
			t.Fatalf("unverified audio/native voice upload advertised through %s", got)
		}
	}
}

type boundedMatrixMedia struct {
	bridgev2.MatrixAPI
	path        string
	usedBounded bool
	usedBytes   bool
}

func (b *boundedMatrixMedia) DownloadMedia(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
	b.usedBytes = true
	return nil, errors.New("unbounded byte download must not be used")
}
func (b *boundedMatrixMedia) DownloadMediaToFile(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo, writable bool, callback func(*os.File) error) error {
	b.usedBounded = true
	f, err := os.Open(b.path)
	if err != nil {
		return err
	}
	defer f.Close()
	return callback(f)
}
func TestMediaFidelityOutgoingDownloadEnforcesSizeLimit(t *testing.T) {
	for _, size := range []int64{0, 3, int64(maxInlineMediaDownload) + 1} {
		t.Run(fmtSize(size), func(t *testing.T) {
			download := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
				if size == 3 {
					w.Write([]byte("abc"))
				} else {
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer download.Close()
			client, err := mautrix.NewClient(download.URL, "@fixture:test", "synthetic")
			if err != nil {
				t.Fatal(err)
			}
			transfers := 0
			s := newSidecarTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				transfers++
				mid := int64(11)
				writeConnectorSidecarResult(t, w, "message", sidecar.MessageMutation{State: sidecar.TransactionCompleted, MessageID: &mid})
			}))
			defer s.Close()
			bot := &matrix.ASIntent{Matrix: &appservice.IntentAPI{Client: client}}
			ic := richClient()
			ic.UserLogin.Bridge.Bot = bot
			ic.Sidecar = sidecar.NewClient(s.URL)
			_, err = ic.HandleMatrixMessage(t.Context(), &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Portal: testPortal("7"), Content: &event.MessageEventContent{MsgType: event.MsgFile, URL: "mxc://fixture/file", Body: "fixture.bin"}}})
			if size == 3 {
				if err != nil || transfers != 1 {
					t.Fatalf("valid file rejected: %v transfers%d", err, transfers)
				}
			} else if err == nil || transfers != 0 {
				t.Fatalf("zero/oversize file transferred: %v transfers%d", err, transfers)
			}
		})
	}
}

func TestMediaFidelityRealMatrixDownloadRejectsOversizeBeforeSpooling(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(maxInlineMediaDownload+1))
		w.WriteHeader(http.StatusOK)
	}))
	defer s.Close()
	client, err := mautrix.NewClient(s.URL, "@fixture:test", "synthetic")
	if err != nil {
		t.Fatal(err)
	}
	raw := &matrix.ASIntent{Matrix: &appservice.IntentAPI{Client: client}}
	_, err = downloadBoundedMatrixMedia(t.Context(), raw, &event.MessageEventContent{URL: "mxc://fixture/file"})
	if err == nil || !isPermanentMediaFailure(err) {
		t.Fatalf("oversize Matrix response was spooled or accepted instead of rejected at headers: %v", err)
	}
}

func TestMediaFidelityMatrixDownloadClassifiesHTTPFailures(t *testing.T) {
	for _, tc := range []struct {
		status    int
		permanent bool
	}{
		{http.StatusNotFound, true},
		{http.StatusRequestTimeout, false},
		{http.StatusTooEarly, false},
		{http.StatusTooManyRequests, false},
		{http.StatusServiceUnavailable, false},
	} {
		t.Run(strconv.Itoa(tc.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"errcode":"M_UNKNOWN","error":"synthetic failure"}`))
			}))
			defer server.Close()
			client, err := mautrix.NewClient(server.URL, "@fixture:test", "synthetic")
			if err != nil {
				t.Fatal(err)
			}
			raw := &matrix.ASIntent{Matrix: &appservice.IntentAPI{Client: client}}
			_, err = downloadBoundedMatrixMedia(t.Context(), raw, &event.MessageEventContent{URL: "mxc://fixture/file"})
			if err == nil || isPermanentMediaFailure(err) != tc.permanent {
				t.Fatalf("HTTP %d: error=%v, want permanent=%t", tc.status, err, tc.permanent)
			}
		})
	}
}

func TestMediaFidelityMatrixDownloadAcceptsExactLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(maxInlineMediaDownload))
		_, _ = io.CopyN(w, zeroMediaReader{}, int64(maxInlineMediaDownload))
	}))
	defer server.Close()
	client, err := mautrix.NewClient(server.URL, "@fixture:test", "synthetic")
	if err != nil {
		t.Fatal(err)
	}
	raw := &matrix.ASIntent{Matrix: &appservice.IntentAPI{Client: client}}
	data, err := downloadBoundedMatrixMedia(t.Context(), raw, &event.MessageEventContent{
		URL: "mxc://fixture/file", Info: &event.FileInfo{Size: maxInlineMediaDownload},
	})
	if err != nil || len(data) != maxInlineMediaDownload {
		t.Fatalf("exact-limit Matrix media: bytes=%d error=%v", len(data), err)
	}
}

type zeroMediaReader struct{}

func (zeroMediaReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func TestMediaFidelityRealMatrixEncryptedDownloadVerifiesHash(t *testing.T) {
	for _, tampered := range []bool{false, true} {
		t.Run(strconv.FormatBool(tampered), func(t *testing.T) {
			plain := []byte("synthetic encrypted media")
			file := attachment.NewEncryptedFile()
			ciphertext := file.Encrypt(plain)
			// Reopen wire metadata, as a received Matrix event does. The
			// encryption object's private decoded cache is not wire state.
			wire, _ := json.Marshal(file)
			var reopened attachment.EncryptedFile
			if err := json.Unmarshal(wire, &reopened); err != nil {
				t.Fatal(err)
			}
			if tampered {
				ciphertext[0] ^= 1
			}
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(ciphertext) }))
			defer s.Close()
			client, err := mautrix.NewClient(s.URL, "@fixture:test", "synthetic")
			if err != nil {
				t.Fatal(err)
			}
			raw := &matrix.ASIntent{Matrix: &appservice.IntentAPI{Client: client}}
			got, err := downloadBoundedMatrixMedia(t.Context(), raw, &event.MessageEventContent{File: &event.EncryptedFileInfo{EncryptedFile: reopened, URL: "mxc://fixture/file"}})
			if tampered {
				if err == nil || len(got) != 0 {
					t.Fatal("corrupt encrypted media accepted")
				}
			} else if err != nil || !bytes.Equal(got, plain) {
				t.Fatalf("valid encrypted media changed: %q %v", got, err)
			}
		})
	}
}

func TestMediaFidelityUnknownMatrixBackendRejectsUnboundedSpool(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture.bin")
	if err := os.WriteFile(path, []byte("abc"), 0600); err != nil {
		t.Fatal(err)
	}
	api := &boundedMatrixMedia{path: path}
	_, err := downloadBoundedMatrixMedia(t.Context(), api, &event.MessageEventContent{URL: "mxc://fixture/file"})
	if err == nil || !isPermanentMediaFailure(err) || api.usedBounded {
		t.Fatalf("unverified backend spooled before rejection: %v used=%v", err, api.usedBounded)
	}
}
func fmtSize(size int64) string {
	switch size {
	case 0:
		return "empty"
	case 3:
		return "valid"
	default:
		return "oversize"
	}
}

// The native video contract requires positive signed-i32 dimensions and a
// positive duration whose ceiling in seconds fits i32. No scalar is clamped.
func TestMediaFidelityVideoMetadataProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		w := hegel.Draw(ht, hegel.Integers[int](math.MinInt, math.MaxInt))
		h := hegel.Draw(ht, hegel.Integers[int](math.MinInt, math.MaxInt))
		d := hegel.Draw(ht, hegel.Integers[int](math.MinInt, math.MaxInt))
		c := &event.MessageEventContent{MsgType: event.MsgVideo, Info: &event.FileInfo{Width: w, Height: h, Duration: d}}
		want := "document"
		if w > 0 && int64(w) <= math.MaxInt32 && h > 0 && int64(h) <= math.MaxInt32 && d > 0 && int64(d) <= int64(math.MaxInt32)*1000 {
			want = "video"
		}
		if got := sidecarUploadKindForMatrix(c); got != want {
			ht.Fatalf("video metadata %d/%d/%d => %s want %s", w, h, d, got, want)
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
	if got := sidecarUploadKindForMatrix(&event.MessageEventContent{MsgType: event.MsgVideo}); got != "document" {
		t.Fatalf("missing video metadata => %s", got)
	}
}

func TestMediaFidelityValidNativeVideoMetadataProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		w := hegel.Draw(ht, hegel.Integers[int](1, math.MaxInt32))
		h := hegel.Draw(ht, hegel.Integers[int](1, math.MaxInt32))
		d := hegel.Draw(ht, hegel.Integers[int64](1, int64(math.MaxInt32)*1000))
		if got := sidecarUploadKindForMatrix(&event.MessageEventContent{MsgType: event.MsgVideo, Info: &event.FileInfo{Width: w, Height: h, Duration: int(d)}}); got != "video" {
			ht.Fatalf("valid native video %d/%d/%d became %s", w, h, d, got)
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}

func TestMediaFidelityEmptyDownloadIsTerminal(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer s.Close()
	if _, err := downloadInlineMedia(t.Context(), s.URL); err == nil || !isPermanentMediaFailure(err) {
		t.Fatalf("empty media accepted or retried: %v", err)
	}
}

func TestMediaFidelityVoiceHasSourceBackedMetadata(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("OggS fixture audio")) }))
	defer s.Close()
	duration := uint64(1250)
	data := sidecar.MessageRecord{Content: sidecar.MessageContent{Type: "media", Kind: "voice", URL: s.URL, MimeType: stringPtr("audio/ogg"), DurationMS: &duration}}
	got, err := convertInlineMessage(t.Context(), testPortal("7"), &richMediaIntent{}, data)
	if err != nil {
		t.Fatal(err)
	}
	c := got.Parts[0].Content
	if c.MsgType != event.MsgAudio || c.MSC3245Voice == nil || c.MSC1767Audio == nil || c.MSC1767Audio.Duration != 1250 || len(c.MSC1767Audio.Waveform) != 0 {
		t.Fatalf("native voice lost metadata or invented waveform: %+v", c)
	}
}

func TestMediaFidelityConflictingMimeUsesFileFallback(t *testing.T) {
	for _, c := range []sidecar.MessageContent{{Kind: "photo", MimeType: stringPtr("video/mp4")}, {Kind: "voice", MimeType: stringPtr("image/png")}, {Kind: "video", MimeType: stringPtr("audio/ogg")}} {
		if got := inlineMediaMessageType(c); got != event.MsgFile {
			t.Fatalf("conflicting media %s/%s became %s", c.Kind, *c.MimeType, got)
		}
	}
}

func TestMediaFidelityPhotoWithConflictingMIMEAndExtension(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("synthetic media"))
	}))
	defer server.Close()
	content := sidecar.MessageContent{
		Type: "media", Kind: "photo", URL: server.URL,
		FileName: stringPtr("image.png"), MimeType: stringPtr("video/mp4"),
	}
	intent := &richMediaIntent{}
	converted, err := convertInlineMessage(t.Context(), testPortal("7"), intent, sidecar.MessageRecord{Content: content})
	if err != nil {
		t.Fatal(err)
	}
	if len(converted.Parts) != 1 || converted.Parts[0].Content.MsgType != event.MsgFile ||
		converted.Parts[0].Content.FileName != "image.png" || converted.Parts[0].Content.Info.MimeType != "video/mp4" || intent.uploads != 1 {
		t.Fatalf("conflicting photo MIME/extension was not preserved as a file: %+v", converted.Parts)
	}
}

// Photo is the only source-backed image representation exposed to the bridge;
// the client record does not distinguish photo-backed stickers from photos.
func TestMediaFidelityPhotoBackedImageFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("synthetic image"))
	}))
	defer server.Close()
	intent := &richMediaIntent{}
	content := sidecar.MessageContent{Type: "media", Kind: "photo", URL: server.URL, MimeType: stringPtr("image/webp")}
	converted, err := convertInlineMessage(t.Context(), testPortal("7"), intent, sidecar.MessageRecord{Content: content})
	if err != nil {
		t.Fatal(err)
	}
	if len(converted.Parts) != 1 || converted.Parts[0].Content.MsgType != event.MsgImage || intent.uploads != 1 {
		t.Fatalf("photo-backed image did not project as an image: %+v", converted.Parts)
	}
}

func TestMediaFidelityFirstEchoPreservesNativeDocumentFallback(t *testing.T) {
	ic := richClient()
	mid := int64(11)
	response, err := ic.matrixSendResponse(&bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Content: &event.MessageEventContent{MsgType: event.MsgVideo, Body: "clip.mp4", FileName: "clip.mp4", URL: "mxc://test/original"}}}, 7, &sidecar.MessageMutation{State: sidecar.TransactionCompleted, MessageID: &mid})
	if err != nil {
		t.Fatal(err)
	}
	data := sidecar.MessageRecord{ChatID: 7, MessageID: 11, Content: sidecar.MessageContent{Type: "media", Kind: "document", FileID: "native-file", MimeType: stringPtr("video/mp4")}}
	edit, err := convertInlineMessageEdit(t.Context(), testPortal("7"), &richMediaIntent{}, []*database.Message{response.DB}, data)
	if err != nil || len(edit.ModifiedParts) != 1 || edit.ModifiedParts[0].Content.MsgType != event.MsgFile || edit.ModifiedParts[0].Content.URL != "mxc://test/original" {
		t.Fatalf("native document fallback was misrepresented or retransferred: %+v %v", edit, err)
	}
}

func TestMediaFidelityFallbackEchoPreservesCaptionEditEnvelope(t *testing.T) {
	for _, msgType := range []event.MessageType{event.MsgAudio, event.MsgVideo} {
		t.Run(string(msgType), func(t *testing.T) {
			original := &event.MessageEventContent{
				MsgType: msgType, Body: "original", FileName: "clip.bin",
				URL: "mxc://test/original", Info: &event.FileInfo{MimeType: "application/octet-stream"},
			}
			ic := richClient()
			mid := int64(11)
			response, err := ic.matrixSendResponse(&bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Content: original}}, 7, &sidecar.MessageMutation{State: sidecar.TransactionCompleted, MessageID: &mid})
			if err != nil {
				t.Fatal(err)
			}
			echo := sidecar.MessageRecord{ChatID: 7, MessageID: 11, Content: sidecar.MessageContent{Type: "media", Kind: "document", FileID: "native-file", Caption: stringPtr("caption")}}
			intent := &richMediaIntent{}
			edit, err := convertInlineMessageEdit(t.Context(), testPortal("7"), intent, []*database.Message{response.DB}, echo)
			if err != nil || len(edit.ModifiedParts) != 1 || edit.ModifiedParts[0].Content.MsgType != event.MsgFile || intent.uploads != 0 {
				t.Fatalf("document fallback echo failed: edit=%+v err=%v uploads=%d", edit, err, intent.uploads)
			}
			captionEdit := *original
			captionEdit.Body = "updated caption"
			if !matrixCaptionEditAllowed(edit.ModifiedParts[0].Part, &captionEdit) {
				t.Fatal("fallback echo lost the original caption-edit envelope")
			}
			captionEdit.URL = "mxc://test/replacement"
			if matrixCaptionEditAllowed(edit.ModifiedParts[0].Part, &captionEdit) {
				t.Fatal("fallback echo allowed replacement media")
			}
		})
	}
}
