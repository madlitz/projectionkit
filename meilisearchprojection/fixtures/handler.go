package fixtures

import (
	"context"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/meilisearch/meilisearch-go"
)

// MessageHandler is a test implementation of boltdb.MessageHandler.
type MessageHandler struct {
	ConfigureFunc   func(c dogma.ProjectionConfigurer)
	HandleEventFunc func(context.Context, meilisearch.ServiceManager, dogma.ProjectionEventScope, dogma.Event) error
	TimeoutHintFunc func(m dogma.Event) time.Duration
	CompactFunc     func(context.Context, meilisearch.ServiceManager, dogma.ProjectionCompactScope) error
}

// Configure configures the behavior of the engine as it relates to this
// handler.
//
// c provides access to the various configuration options, such as specifying
// which types of event messages are routed to this handler.
//
// If h.ConfigureFunc is non-nil, it calls h.ConfigureFunc(c).
func (h *MessageHandler) Configure(c dogma.ProjectionConfigurer) {
	if h.ConfigureFunc != nil {
		h.ConfigureFunc(c)
	}
}

// HandleEvent handles a domain event message that has been routed to this
// handler.
//
// If h.HandleEventFunc is non-nil it returns h.HandleEventFunc(ctx, tx, s, m).
func (h *MessageHandler) HandleEvent(
	ctx context.Context,
	tx meilisearch.ServiceManager,
	s dogma.ProjectionEventScope,
	m dogma.Event,
) error {
	if h.HandleEventFunc != nil {
		return h.HandleEventFunc(ctx, tx, s, m)
	}

	return nil
}

// TimeoutHint returns a duration that is suitable for computing a deadline for
// the handling of the given message by this handler.
//
// If h.TimeoutHintFunc is non-nil it returns h.TimeoutHintFunc(m), otherwise it
// returns 0.
func (h *MessageHandler) TimeoutHint(m dogma.Event) time.Duration {
	if h.TimeoutHintFunc != nil {
		return h.TimeoutHintFunc(m)
	}

	return 0
}

// Compact reduces the size of the projection's data.
//
// If h.CompactFunc is non-nil it returns h.CompactFunc(ctx,db,s), otherwise it
// returns nil.
func (h *MessageHandler) Compact(ctx context.Context, db meilisearch.ServiceManager, s dogma.ProjectionCompactScope) error {
	if h.CompactFunc != nil {
		return h.CompactFunc(ctx, db, s)
	}

	return nil
}
