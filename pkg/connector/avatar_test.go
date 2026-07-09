package connector

import (
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

func TestPinRemoteNameProfileHandlesNilLogin(t *testing.T) {
	if pinRemoteNameProfile(nil) {
		t.Fatal("pinRemoteNameProfile(nil) changed = true, want false")
	}
}

func TestPinRemoteNameProfileUsesInlineDisplayName(t *testing.T) {
	ul := &bridgev2.UserLogin{UserLogin: &database.UserLogin{
		RemoteName: "Inline 16000",
	}}

	if !pinRemoteNameProfile(ul) {
		t.Fatal("pinRemoteNameProfile changed = false, want true")
	}
	if ul.RemoteName != inlineRemoteDisplayName {
		t.Fatalf("RemoteName = %q, want %q", ul.RemoteName, inlineRemoteDisplayName)
	}
	if ul.RemoteProfile.Name != inlineRemoteDisplayName {
		t.Fatalf("RemoteProfile.Name = %q, want %q", ul.RemoteProfile.Name, inlineRemoteDisplayName)
	}
	if pinRemoteNameProfile(ul) {
		t.Fatal("pinRemoteNameProfile changed = true after profile was already pinned")
	}
}
