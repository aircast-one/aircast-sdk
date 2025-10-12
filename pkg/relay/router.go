package relay

import (
	"context"
	"fmt"
	"reflect"

	"github.com/pavliha/aircast-sdk/pkg/message"
	"github.com/sirupsen/logrus"
)

// ActionHandler processes an action with the given context, request, and response.
type ActionHandler func(ctx context.Context, req *Request, res ResponseWriter) error

// EventHandler processes an event with the given context and event data.
type EventHandler func(ctx context.Context, event *EventRequest) error

// Middleware wraps an ActionHandler for pre- and post-processing.
type Middleware func(ActionHandler) ActionHandler

// EventMiddleware wraps an EventHandler for pre- and post-processing.
type EventMiddleware func(EventHandler) EventHandler

// ResponseFactory creates a ResponseWriter for the given request.
// Services implement this to provide their specific response routing logic.
type ResponseFactory func(ctx context.Context, req *Request, logger *logrus.Entry) ResponseWriter

// ErrorSender sends error responses when no handler is found or validation fails.
type ErrorSender func(ctx context.Context, req *Request, code, message string) error

// Router stores registered action handlers and global middleware.
type Router struct {
	routes           map[string]ActionHandler // action name → handler
	middlewares      []Middleware             // global middleware stack
	eventRoutes      map[string]EventHandler  // event action name → handler
	eventMiddlewares []EventMiddleware        // event middleware stack
	responseFactory  ResponseFactory          // creates responses for handlers
	errorSender      ErrorSender              // sends errors when no handler found
	logger           *logrus.Entry
}

// NewRouter creates a new Router with the given logger, response factory, and error sender.
func NewRouter(logger *logrus.Entry, responseFactory ResponseFactory, errorSender ErrorSender) *Router {
	return &Router{
		routes:           make(map[string]ActionHandler),
		middlewares:      []Middleware{},
		eventRoutes:      make(map[string]EventHandler),
		eventMiddlewares: []EventMiddleware{},
		responseFactory:  responseFactory,
		errorSender:      errorSender,
		logger:           logger.WithField("component", "router"),
	}
}

// UseRequestMiddleware adds global middleware to the request stack. Middlewares are executed in the order added.
func (r *Router) UseRequestMiddleware(mw Middleware) {
	r.middlewares = append(r.middlewares, mw)
}

// UseEventMiddleware adds middleware to the event processing chain
func (r *Router) UseEventMiddleware(mw EventMiddleware) {
	r.eventMiddlewares = append(r.eventMiddlewares, mw)
}

// HandleRequest registers an action with optional inline middleware and a final ActionHandler.
// The last argument must be an ActionHandler or a convertible function; any preceding
// arguments must be Middleware.
// Global middlewares wrap all registered handlers.
func (r *Router) HandleRequest(action string, components ...any) {
	if len(components) == 0 {
		panic(fmt.Sprintf("no handler provided for action %s", action))
	}

	// Adapt the final component into an ActionHandler
	var handler ActionHandler
	last := components[len(components)-1]
	switch fn := last.(type) {
	case ActionHandler:
		handler = fn
	default:
		if adapted := r.adaptHandler(fn); adapted != nil {
			handler = adapted
		} else {
			panic(fmt.Sprintf("last component for action %s is not an ActionHandler", action))
		}
	}

	// Apply inline middleware (from left to right)
	for _, comp := range components[:len(components)-1] {
		mw, ok := comp.(Middleware)
		if !ok {
			panic(fmt.Sprintf("component for action %s is not middleware", action))
		}
		handler = mw(handler)
	}

	// Wrap with global middlewares in registration order
	for _, mw := range r.middlewares {
		handler = mw(handler)
	}

	r.routes[action] = handler
	r.logger.WithField("action", action).Trace("Registered request handler")
}

// HandleEvent registers an event handler with middleware applied
func (r *Router) HandleEvent(action string, handler EventHandler) {
	// Apply all middlewares to the handler
	wrappedHandler := handler
	for _, mw := range r.eventMiddlewares {
		wrappedHandler = mw(wrappedHandler)
	}
	r.eventRoutes[action] = wrappedHandler
	r.logger.WithField("action", action).Trace("Registered event handler")
}

// GetHandler retrieves the ActionHandler for the given action.
func (r *Router) GetHandler(action string) (ActionHandler, bool) {
	handler, found := r.routes[action]
	return handler, found
}

// ProcessRequest processes a request message
func (r *Router) ProcessRequest(ctx context.Context, m message.RequestMessage) error {
	r.logger.WithFields(map[string]any{
		"action":     m.Action,
		"request_id": m.RequestID,
		"session_id": m.ChannelID,
		"source":     m.Source,
	}).Debug("Processing request message")

	req, err := CreateFromRequestMessage(m)
	if err != nil {
		r.logger.WithError(err).Error("Invalid request message")
		return err
	}

	handlerFunc, exists := r.routes[req.Action]
	if !exists {
		r.logger.WithField("action", req.Action).Warn("No handler found for request action")
		return r.errorSender(ctx, req, "INVALID_REQUEST", fmt.Sprintf("Unknown action %q", req.Action))
	}

	r.logger.WithField("action", req.Action).Debug("Found handler, executing...")

	resp := r.responseFactory(ctx, req, r.logger)
	if err := handlerFunc(ctx, req, resp); err != nil {
		r.logger.WithError(err).WithField("action", req.Action).Error("Failed to handle request")
		return err
	}

	r.logger.WithField("action", req.Action).Debug("Request handled successfully")
	return nil
}

// ProcessEvent processes an event message
func (r *Router) ProcessEvent(ctx context.Context, m message.EventMessage) error {
	r.logger.WithFields(map[string]any{
		"action":     m.Action,
		"session_id": m.ChannelID,
		"source":     m.Source,
	}).Debug("Processing event message")

	handlerFunc, exists := r.eventRoutes[m.Action]
	if !exists {
		r.logger.WithField("action", m.Action).Debug("No handler registered for event action")
		return nil
	}

	r.logger.WithField("action", m.Action).Debug("Found event handler, executing...")

	// Create an EventRequest for consistent payload processing
	eventReq := &EventRequest{
		Action:       m.Action,
		SessionID:    m.ChannelID,
		Payload:      m.Payload,
		Source:       m.Source,
		TraceContext: m.TraceContext, // Preserve W3C Trace Context for distributed tracing
	}

	if err := handlerFunc(ctx, eventReq); err != nil {
		r.logger.WithError(err).WithField("action", m.Action).Error("Failed to handle event")
		return err
	}

	r.logger.WithField("action", m.Action).Debug("Event handled successfully")
	return nil
}

// adaptHandler attempts to convert various function signatures into an ActionHandler.
func (r *Router) adaptHandler(candidate any) ActionHandler {
	// Already the right type?
	if ah, ok := candidate.(ActionHandler); ok {
		return ah
	}

	// Check if it's a function type we can adapt
	typ := reflect.TypeOf(candidate)
	if typ.Kind() != reflect.Func {
		return nil
	}

	// Try reflection-based adapter for full signature
	if adapter := r.tryReflectionAdapter(candidate, typ); adapter != nil {
		return adapter
	}

	// Try simple function adapters
	return r.trySimpleFunctionAdapters(candidate)
}

// tryReflectionAdapter attempts to adapt a function with the full ActionHandler signature
func (r *Router) tryReflectionAdapter(candidate any, typ reflect.Type) ActionHandler {
	if !r.isValidActionHandlerSignature(typ) {
		return nil
	}

	return func(ctx context.Context, req *Request, res ResponseWriter) error {
		outs := reflect.ValueOf(candidate).Call([]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(req),
			reflect.ValueOf(res),
		})

		if errVal := outs[0]; !errVal.IsNil() {
			return errVal.Interface().(error)
		}
		return nil
	}
}

// isValidActionHandlerSignature checks if the function signature matches ActionHandler requirements
func (r *Router) isValidActionHandlerSignature(typ reflect.Type) bool {
	return typ.NumIn() == 3 && typ.NumOut() == 1 &&
		typ.In(0).String() == "context.Context" &&
		typ.In(1).AssignableTo(reflect.TypeOf(&Request{})) &&
		typ.In(2).Implements(reflect.TypeOf((*ResponseWriter)(nil)).Elem()) &&
		typ.Out(0).AssignableTo(reflect.TypeOf((*error)(nil)).Elem())
}

// trySimpleFunctionAdapters attempts to adapt simple function signatures
func (r *Router) trySimpleFunctionAdapters(candidate any) ActionHandler {
	// Try each adapter type
	if adapter := r.adaptNoArgFunction(candidate); adapter != nil {
		return adapter
	}
	if adapter := r.adaptNoArgWithErrorFunction(candidate); adapter != nil {
		return adapter
	}
	if adapter := r.adaptSingleArgFunction(candidate); adapter != nil {
		return adapter
	}
	if adapter := r.adaptSingleArgWithErrorFunction(candidate); adapter != nil {
		return adapter
	}

	return nil
}

// adaptNoArgFunction adapts func() any
func (r *Router) adaptNoArgFunction(candidate any) ActionHandler {
	if fn, ok := candidate.(func() any); ok {
		return func(ctx context.Context, req *Request, res ResponseWriter) error {
			result := fn()
			return res.SendSuccess(result)
		}
	}
	return nil
}

// adaptNoArgWithErrorFunction adapts func() (any, error)
func (r *Router) adaptNoArgWithErrorFunction(candidate any) ActionHandler {
	if fn, ok := candidate.(func() (any, error)); ok {
		return func(ctx context.Context, req *Request, res ResponseWriter) error {
			result, err := fn()
			if err != nil {
				return res.SendError("SERVICE_UNAVAILABLE", err.Error())
			}
			return res.SendSuccess(result)
		}
	}
	return nil
}

// adaptSingleArgFunction adapts func(any) any
func (r *Router) adaptSingleArgFunction(candidate any) ActionHandler {
	if fn, ok := candidate.(func(any) any); ok {
		return func(ctx context.Context, req *Request, res ResponseWriter) error {
			result := fn(req.Payload)
			return res.SendSuccess(result)
		}
	}
	return nil
}

// adaptSingleArgWithErrorFunction adapts func(any) (any, error)
func (r *Router) adaptSingleArgWithErrorFunction(candidate any) ActionHandler {
	if fn, ok := candidate.(func(any) (any, error)); ok {
		return func(ctx context.Context, req *Request, res ResponseWriter) error {
			result, err := fn(req.Payload)
			if err != nil {
				return res.SendError("SERVICE_UNAVAILABLE", err.Error())
			}
			return res.SendSuccess(result)
		}
	}
	return nil
}
