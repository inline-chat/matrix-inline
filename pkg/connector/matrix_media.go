package connector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type matrixMediaDownloader interface {
	Download(context.Context, id.ContentURI) (*http.Response, error)
}

func (ic *InlineClient) downloadMatrixMedia(ctx context.Context, content *event.MessageEventContent) ([]byte, error) {
	if ic.UserLogin == nil || ic.UserLogin.Bridge == nil {
		return nil, fmt.Errorf("Matrix media transport is unavailable")
	}
	intent, ok := ic.UserLogin.Bridge.Bot.(*matrix.ASIntent)
	if !ok || intent.Matrix == nil {
		return nil, fmt.Errorf("Matrix media transport does not support bounded authenticated downloads")
	}
	return downloadBoundedMatrixMedia(ctx, intent.Matrix, content, maxInlineMediaDownload)
}

// Use the homeserver client's configured authenticated MXC endpoint. Reading a
// limited response here avoids the framework's unbounded bytes/file helpers.
func downloadBoundedMatrixMedia(ctx context.Context, client matrixMediaDownloader, content *event.MessageEventContent, maxBytes int64) ([]byte, error) {
	if content == nil {
		return nil, permanentMediaFailure(fmt.Errorf("missing Matrix attachment"))
	}
	if content.Info != nil && int64(content.Info.Size) > maxBytes {
		return nil, permanentMediaFailure(fmt.Errorf("Matrix media declared size exceeds %d byte limit", maxBytes))
	}
	uri := content.URL
	if content.File != nil {
		uri = content.File.URL
		if err := content.File.PrepareForDecryption(); err != nil {
			return nil, permanentMediaFailure(fmt.Errorf("invalid Matrix encryption metadata: %w", err))
		}
	}
	parsed, err := uri.Parse()
	if err != nil || parsed.IsEmpty() {
		return nil, permanentMediaFailure(fmt.Errorf("invalid Matrix media URI"))
	}
	response, err := client.Download(ctx, parsed)
	if err != nil {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		var httpErr mautrix.HTTPError
		if errors.As(err, &httpErr) && httpErr.Response != nil && terminalMediaStatus(httpErr.Response.StatusCode) {
			return nil, permanentMediaFailure(err)
		}
		return nil, err
	}
	if response == nil || response.Body == nil {
		return nil, fmt.Errorf("Matrix media transport returned no response body")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		err = fmt.Errorf("Matrix media returned HTTP %d", response.StatusCode)
		if terminalMediaStatus(response.StatusCode) {
			return nil, permanentMediaFailure(err)
		}
		return nil, err
	}
	if response.ContentLength > maxBytes {
		return nil, permanentMediaFailure(fmt.Errorf("Matrix media Content-Length exceeds %d byte limit", maxBytes))
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read Matrix attachment: %w", err)
	}
	if int64(len(data)) > maxBytes {
		return nil, permanentMediaFailure(fmt.Errorf("Matrix media exceeds %d byte limit", maxBytes))
	}
	if len(data) == 0 {
		return nil, permanentMediaFailure(fmt.Errorf("Matrix media contains no bytes"))
	}
	if content.File != nil {
		// DecryptInPlace verifies SHA-256 of the entire bounded ciphertext before
		// exposing any plaintext to the sidecar, including for chunked responses.
		if err := content.File.DecryptInPlace(data); err != nil {
			return nil, permanentMediaFailure(fmt.Errorf("verify Matrix attachment: %w", err))
		}
	}
	return data, nil
}
func terminalMediaStatus(status int) bool {
	return status >= 400 && status < 500 && status != 408 && status != 425 && status != 429
}
