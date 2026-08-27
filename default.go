package chronos

import (
	"sync/atomic"
)

var defaultClient atomic.Pointer[Client]

// Default returns the process-wide client from the last successful Start/StartWithConfig
// with Background enabled, or a disabled no-op client.
func Default() *Client {
	if c := defaultClient.Load(); c != nil {
		return c
	}
	return &Client{}
}

func setDefault(c *Client) {
	if c == nil || !c.cfg.Enabled {
		return
	}
	defaultClient.Store(c)
}
