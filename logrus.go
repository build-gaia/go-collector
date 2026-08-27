package chronos

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
)

// LogrusHook mirrors logrus entries into Chronos .log batches.
// Attach with logger.AddHook(client.LogrusHook()).
type LogrusHook struct {
	Client *Client
}

// Levels reports levels this hook observes.
func (h *LogrusHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire writes a Chronos log record. Fail-open.
func (h *LogrusHook) Fire(entry *logrus.Entry) error {
	if h == nil || h.Client == nil || !h.Client.cfg.Enabled || !h.Client.cfg.LogsEnabled {
		return nil
	}
	attrs := map[string]string{}
	for k, v := range entry.Data {
		if len(attrs) >= 64 {
			break
		}
		attrs[k] = stringifyField(v)
	}
	ctx := entry.Context
	if ctx == nil {
		ctx = context.Background()
	}
	h.Client.Log(ctx, entry.Level.String(), entry.Message, attrs)
	return nil
}

// LogrusHook returns a hook bound to this client.
func (c *Client) LogrusHook() *LogrusHook {
	return &LogrusHook{Client: c}
}

func stringifyField(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case error:
		return t.Error()
	default:
		return fmt.Sprint(v)
	}
}
