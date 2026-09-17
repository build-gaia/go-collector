package chronos

import "testing"

// Payload capture is estate-wide behaviour, so the unprefixed key is the one to
// use. The CHRONOS_GO_* spelling predates that and must keep working: .chronos
// files and compose files across the estate still carry it.
func TestCaptureKeysAcceptBothSpellings(t *testing.T) {
	t.Run("unprefixed key is read", func(t *testing.T) {
		t.Setenv("CHRONOS_MESSAGING_CAPTURE_BODIES", "false")
		if LoadConfig().MessagingCaptureBodies {
			t.Error("CHRONOS_MESSAGING_CAPTURE_BODIES=false was ignored")
		}
	})

	t.Run("deprecated key still works", func(t *testing.T) {
		t.Setenv("CHRONOS_GO_MESSAGING_CAPTURE_BODIES", "false")
		if LoadConfig().MessagingCaptureBodies {
			t.Error("CHRONOS_GO_MESSAGING_CAPTURE_BODIES=false was ignored")
		}
	})

	t.Run("unprefixed key wins when both are set", func(t *testing.T) {
		t.Setenv("CHRONOS_GO_MESSAGING_CAPTURE_BODIES", "false")
		t.Setenv("CHRONOS_MESSAGING_CAPTURE_BODIES", "true")
		if !LoadConfig().MessagingCaptureBodies {
			t.Error("the deprecated key overrode the estate-wide one")
		}
	})

	t.Run("http bodies follow the same rule", func(t *testing.T) {
		t.Setenv("CHRONOS_GO_HTTP_CAPTURE_BODIES", "true")
		t.Setenv("CHRONOS_HTTP_CAPTURE_BODIES", "false")
		if LoadConfig().HTTPCaptureBodies {
			t.Error("the deprecated key overrode the estate-wide one")
		}
	})

	t.Run("neither set leaves the default", func(t *testing.T) {
		if !LoadConfig().MessagingCaptureBodies {
			t.Error("capture should default on")
		}
	})
}
