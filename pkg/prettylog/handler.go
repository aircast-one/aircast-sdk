// Package prettylog provides a colored slog.Handler for terminal output.
//
// Short log lines stay single-line:
//
//	[23:55:45] INFO: Application started component=Application
//
// Long lines (>120 visible chars) break into pino-pretty style:
//
//	[23:55:46] WARN: Subscribe failed: device not connected
//	    device_id: 812d77ed-0d0b-4a62-a9d8-46ca34a22e83
//	    session_id: 5bb6e74e-ed01-461e-a21d-149a86bee17d
package prettylog

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
)

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorGray   = "\033[90m"

	// DefaultMaxSingleLine is the visible-character threshold.
	// Lines shorter than this stay on one line; longer lines break into multi-line.
	DefaultMaxSingleLine = 120
)

// Handler renders slog records as compact colored lines for terminals.
type Handler struct {
	w              io.Writer
	mu             sync.Mutex
	level          slog.Leveler
	attrs          []slog.Attr
	group          string
	suppressedKeys map[string]bool
	maxSingleLine  int
}

// Option configures a Handler.
type Option func(*Handler)

// WithSuppressedKeys hides the given keys from output.
func WithSuppressedKeys(keys ...string) Option {
	return func(h *Handler) {
		for _, k := range keys {
			h.suppressedKeys[k] = true
		}
	}
}

// WithMaxSingleLine sets the visible-character threshold for single-line output.
func WithMaxSingleLine(n int) Option {
	return func(h *Handler) {
		h.maxSingleLine = n
	}
}

// New creates a pretty handler that writes to w.
func New(w io.Writer, opts *slog.HandlerOptions, options ...Option) *Handler {
	h := &Handler{
		w:              w,
		suppressedKeys: make(map[string]bool),
		maxSingleLine:  DefaultMaxSingleLine,
	}
	if opts != nil && opts.Level != nil {
		h.level = opts.Level
	} else {
		h.level = slog.LevelInfo
	}
	for _, o := range options {
		o(h)
	}
	return h
}

func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

func (h *Handler) Handle(_ context.Context, r slog.Record) error {
	lvl, color := levelString(r.Level)

	header := fmt.Sprintf("%s[%s]%s %s%s%s: %s",
		colorGray, r.Time.Format("15:04:05"), colorReset,
		color, lvl, colorReset,
		r.Message,
	)

	type field struct{ key, val string }
	var fields []field

	collect := func(a slog.Attr) {
		key := a.Key
		if h.group != "" {
			key = h.group + "." + key
		}
		if h.suppressedKeys[key] {
			return
		}
		fields = append(fields, field{key, a.Value.Resolve().String()})
	}

	for _, a := range h.attrs {
		collect(a)
	}
	r.Attrs(func(a slog.Attr) bool {
		collect(a)
		return true
	})

	if len(fields) == 0 {
		return h.write(header + "\n")
	}

	// Build inline version
	var inline strings.Builder
	inline.WriteString(header)
	for _, f := range fields {
		fmt.Fprintf(&inline, " %s%s=%s%s", colorGray, f.key, colorReset, f.val)
	}

	if len(stripAnsi(inline.String())) <= h.maxSingleLine {
		return h.write(inline.String() + "\n")
	}

	// Multi-line: header on first line, fields indented below
	var multi strings.Builder
	multi.WriteString(header)
	multi.WriteByte('\n')
	for _, f := range fields {
		fmt.Fprintf(&multi, "    %s%s:%s %s\n", colorGray, f.key, colorReset, f.val)
	}
	return h.write(multi.String())
}

func (h *Handler) write(s string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.w, s)
	return err
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newAttrs := make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(newAttrs, h.attrs)
	copy(newAttrs[len(h.attrs):], attrs)
	return &Handler{
		w:              h.w,
		level:          h.level,
		attrs:          newAttrs,
		group:          h.group,
		suppressedKeys: h.suppressedKeys,
		maxSingleLine:  h.maxSingleLine,
	}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	g := name
	if h.group != "" {
		g = h.group + "." + name
	}
	return &Handler{
		w:              h.w,
		level:          h.level,
		attrs:          h.attrs,
		group:          g,
		suppressedKeys: h.suppressedKeys,
		maxSingleLine:  h.maxSingleLine,
	}
}

func levelString(level slog.Level) (string, string) {
	switch {
	case level >= slog.LevelError:
		return "ERROR", colorRed
	case level >= slog.LevelWarn:
		return "WARN", colorYellow
	case level >= slog.LevelInfo:
		return "INFO", colorGreen
	default:
		return "DEBUG", colorCyan
	}
}

// stripAnsi removes ANSI escape sequences to measure visible width.
func stripAnsi(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	for i < len(s) {
		if s[i] == '\033' && i+1 < len(s) && s[i+1] == '[' {
			j := i + 2
			for j < len(s) && s[j] != 'm' {
				j++
			}
			i = j + 1
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

// IsTerminal checks if the given writer is a terminal.
func IsTerminal(w io.Writer) bool {
	if f, ok := w.(*os.File); ok {
		info, err := f.Stat()
		if err != nil {
			return false
		}
		return (info.Mode() & os.ModeCharDevice) != 0
	}
	return false
}
