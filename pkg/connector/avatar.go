package connector

import (
	"context"
	_ "embed"
	"time"

	"maunium.net/go/mautrix/bridgev2"
)

const (
	inlineLogoFileName      = "inline-logo.png"
	inlineLogoMIMEType      = "image/png"
	remoteProfileSaveWindow = 30 * time.Second
)

//go:embed assets/inline-logo.png
var inlineLogoPNG []byte

func (ic *InlineClient) ensureRemoteProfile(ctx context.Context) {
	if ic == nil || ic.UserLogin == nil {
		return
	}
	ul := ic.UserLogin
	changed := pinRemoteNameProfile(ul)
	if ul.RemoteProfile.Avatar == "" && ul.Bridge != nil && ul.Bridge.Bot != nil && len(inlineLogoPNG) > 0 {
		uploadCtx, cancel := context.WithTimeout(ctx, remoteProfileSaveWindow)
		mxc, _, err := ul.Bridge.Bot.UploadMedia(uploadCtx, "", inlineLogoPNG, inlineLogoFileName, inlineLogoMIMEType)
		cancel()
		if err != nil {
			ul.Bridge.Log.Warn().Err(err).Msg("Failed to upload Inline account avatar")
		} else if mxc != "" {
			ul.RemoteProfile.Avatar = mxc
			changed = true
		}
	}
	if !changed {
		return
	}
	if ul.Bridge == nil {
		return
	}
	saveCtx, cancel := context.WithTimeout(ctx, remoteProfileSaveWindow)
	defer cancel()
	if err := ul.Save(saveCtx); err != nil {
		ul.Bridge.Log.Warn().Err(err).Msg("Failed to save Inline account profile")
	}
}

func pinRemoteNameProfile(ul *bridgev2.UserLogin) bool {
	if ul == nil {
		return false
	}
	changed := false
	if ul.RemoteName != inlineRemoteDisplayName {
		ul.RemoteName = inlineRemoteDisplayName
		changed = true
	}
	if ul.RemoteProfile.Name != inlineRemoteDisplayName {
		ul.RemoteProfile.Name = inlineRemoteDisplayName
		changed = true
	}
	return changed
}
