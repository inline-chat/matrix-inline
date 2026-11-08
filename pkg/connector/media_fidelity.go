package connector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/event"
)

func nativeVideoMetadataValid(info *event.FileInfo) bool {
	return info != nil && info.Width > 0 && int64(info.Width) <= math.MaxInt32 && info.Height > 0 && int64(info.Height) <= math.MaxInt32 && info.Duration > 0 && int64(info.Duration) <= int64(math.MaxInt32)*1000
}

// The pinned framework's file helper spools/preallocates the entire response.
// Use its exported authenticated stream instead: no disk spool, bounded bytes,
// and the same encrypted-file hash verification. Other backends fail explicitly
// until they provide a proven bounded transport. Declared size is only a hint.
func downloadBoundedMatrixMedia(ctx context.Context, intent bridgev2.MatrixAPI, content *event.MessageEventContent) ([]byte, error) {
	if content.Info != nil && content.Info.Size > maxInlineMediaDownload {
		return nil, permanentMediaFailure(fmt.Errorf("Matrix media exceeds %d byte bridge upload limit", maxInlineMediaDownload))
	}
	raw, ok := intent.(*matrix.ASIntent)
	if !ok || raw == nil || raw.Matrix == nil || raw.Matrix.Client == nil {
		return nil, permanentMediaFailure(fmt.Errorf("bounded Matrix media transport unavailable: %w", bridgev2.ErrUnsupportedMessageType))
	}
	uri := content.URL
	file := content.File
	if file != nil {
		copy := *file
		file = &copy
		uri = file.URL
		if err := file.PrepareForDecryption(); err != nil {
			return nil, permanentMediaFailure(fmt.Errorf("invalid encrypted Matrix media: %w", err))
		}
	}
	parsed, err := uri.Parse()
	if err != nil || parsed.IsEmpty() {
		return nil, permanentMediaFailure(fmt.Errorf("invalid Matrix media URI %q", uri))
	}
	response, err := raw.Matrix.Download(ctx, parsed)
	if err != nil {
		var httpError mautrix.HTTPError
		if errors.As(err, &httpError) && httpError.Response != nil {
			status := httpError.Response.StatusCode
			if status >= 400 && status < 500 && status != 408 && status != 425 && status != 429 {
				return nil, permanentMediaFailure(err)
			}
		}
		return nil, err
	}
	defer response.Body.Close()
	if response.ContentLength > maxInlineMediaDownload {
		return nil, permanentMediaFailure(fmt.Errorf("Matrix media exceeds %d byte bridge upload limit", maxInlineMediaDownload))
	}
	var reader io.ReadCloser = response.Body
	if file != nil {
		reader = file.DecryptStream(reader)
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, maxInlineMediaDownload+1))
	closeErr := reader.Close()
	if len(data) > maxInlineMediaDownload {
		return nil, permanentMediaFailure(fmt.Errorf("Matrix media exceeds %d byte bridge upload limit", maxInlineMediaDownload))
	}
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		if file != nil {
			return nil, permanentMediaFailure(fmt.Errorf("encrypted Matrix media verification failed: %w", closeErr))
		}
		return nil, closeErr
	}
	if len(data) == 0 {
		return nil, permanentMediaFailure(fmt.Errorf("Matrix media is empty"))
	}
	return data, nil
}
