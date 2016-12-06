package alert

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"path"
	"strings"
	"sync"

	client "github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/services/httpd"
)

const (
	alertsPath         = "/alerts"
	alertsPathAnchored = "/alerts/"

	topicsPath             = alertsPath + "/topics"
	topicsPathAnchored     = alertsPath + "/topics/"
	topicsBasePath         = httpd.BasePath + topicsPath
	topicsBasePathAnchored = httpd.BasePath + topicsPathAnchored

	handlersPath         = alertsPath + "/handlers"
	handlersPathAnchored = alertsPath + "/handlers/"
	handlersBasePath     = httpd.BasePath + handlersPath

	topicEventsPath   = "events"
	topicHandlersPath = "handlers"

	eventsPattern   = "*/" + topicEventsPath
	eventPattern    = "*/" + topicEventsPath + "/*"
	handlersPattern = "*/" + topicHandlersPath
	handlerPattern  = "*/" + topicHandlersPath + "/*"

	eventsRelation   = "events"
	handlersRelation = "handlers"
)

type handlerFromOptions interface {
	HandlerFromOptions(options map[string]interface{}, l *log.Logger) (Handler, error)
}

type Service struct {
	mu sync.RWMutex

	handlerSpecs map[string]HandlerSpec
	handlers     map[string]Handler

	system *system

	routes       []httpd.Route
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}

	Slack handlerFromOptions

	logger *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		handlerSpecs: make(map[string]HandlerSpec),
		handlers:     make(map[string]Handler),
		system:       newSystem(l),
		logger:       l,
	}
	return s
}

func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Define API routes
	s.routes = []httpd.Route{
		{
			Method:      "GET",
			Pattern:     topicsPath,
			HandlerFunc: s.handleListTopics,
		},
		{
			Method:      "GET",
			Pattern:     topicsPathAnchored,
			HandlerFunc: s.handleRouteTopic,
		},
		{
			Method:      "GET",
			Pattern:     handlersPath,
			HandlerFunc: s.handleListHandlers,
		},
		{
			Method:      "POST",
			Pattern:     handlersPath,
			HandlerFunc: s.handleCreateHandler,
		},
		//{
		//	Method:      "GET",
		//	Pattern:     handlersPathAnchored,
		//	HandlerFunc: s.handleRouteTopic,
		//},
	}

	return s.HTTPDService.AddRoutes(s.routes)
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.system.Close()
	s.HTTPDService.DelRoutes(s.routes)
	return nil
}

func validatePattern(pattern string) error {
	_, err := path.Match(pattern, "")
	return err
}

func (s *Service) handleListTopics(w http.ResponseWriter, r *http.Request) {
	pattern := r.URL.Query().Get("pattern")
	if err := validatePattern(pattern); err != nil {
		httpd.HttpError(w, fmt.Sprint("invalide pattern: ", err.Error()), true, http.StatusBadRequest)
		return
	}
	minLevelStr := r.URL.Query().Get("min-level")
	minLevel, err := ParseLevel(minLevelStr)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	topics := client.Topics{
		Link:   client.Link{Relation: client.Self, Href: r.URL.String()},
		Topics: s.TopicStatus(pattern, minLevel),
	}

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(topics, true))
}

func (s *Service) topicIDFromPath(p string) (id string) {
	d := p
	for d != "." {
		id = d
		d = path.Dir(d)
	}
	return
}

func pathMatch(pattern, p string) (match bool) {
	match, _ = path.Match(pattern, p)
	return
}

func (s *Service) handleRouteTopic(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, topicsBasePathAnchored)
	id := s.topicIDFromPath(p)
	t, ok := s.system.Topic(id)
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("topic %q does not exist", id), true, http.StatusNotFound)
		return
	}

	switch {
	case pathMatch(eventsPattern, p):
		s.handleListTopicEvents(t, w, r)
	case pathMatch(eventPattern, p):
		s.handleTopicEvent(t, w, r)
	case pathMatch(handlersPattern, p):
		s.handleListTopicHandlers(t, w, r)
	case pathMatch(handlerPattern, p):
		s.handleTopicHandler(t, w, r)
	default:
		s.handleTopic(t, w, r)
	}
}

func (s *Service) handlerLink(id string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(handlersBasePath, id)}
}
func (s *Service) topicLink(id string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(topicsBasePath, id)}
}
func (s *Service) topicEventsLink(id string, r client.Relation) client.Link {
	return client.Link{Relation: r, Href: path.Join(topicsBasePath, id, topicEventsPath)}
}
func (s *Service) topicEventLink(topic, event string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(topicsBasePath, topic, topicEventsPath, event)}
}
func (s *Service) topicHandlersLink(id string, r client.Relation) client.Link {
	return client.Link{Relation: r, Href: path.Join(topicsBasePath, id, topicHandlersPath)}
}
func (s *Service) topicHandlerLink(topic, handler string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(topicsBasePath, topic, topicHandlersPath, handler)}
}

func (s *Service) createClientTopic(topic string, level Level) client.Topic {
	return client.Topic{
		ID:           topic,
		Link:         s.topicLink(topic),
		Level:        level.String(),
		EventsLink:   s.topicEventsLink(topic, eventsRelation),
		HandlersLink: s.topicHandlersLink(topic, handlersRelation),
	}
}

func (s *Service) handleTopic(t *Topic, w http.ResponseWriter, r *http.Request) {
	topic := s.createClientTopic(t.ID(), t.MaxLevel())

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(topic, true))
}

func (s *Service) convertEventState(state EventState) client.EventState {
	return client.EventState{
		Message:  state.Message,
		Details:  state.Details,
		Time:     state.Time,
		Duration: state.Duration,
		Level:    state.Level.String(),
	}
}

func (s *Service) handleListTopicEvents(t *Topic, w http.ResponseWriter, r *http.Request) {
	minLevelStr := r.URL.Query().Get("min-level")
	minLevel, err := ParseLevel(minLevelStr)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	events := t.Events(minLevel)
	res := client.TopicEvents{
		Link:   s.topicEventsLink(t.ID(), client.Self),
		Topic:  t.ID(),
		Events: make([]client.Event, 0, len(events)),
	}
	for id, state := range events {
		res.Events = append(res.Events, client.Event{
			Link:  s.topicEventLink(t.ID(), id),
			ID:    id,
			State: s.convertEventState(state),
		})
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(res, true))
}

func (s *Service) handleTopicEvent(t *Topic, w http.ResponseWriter, r *http.Request) {
	id := path.Base(r.URL.Path)
	state, ok := t.EventState(id)
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("event %q does not exist for topic %q", id, t.ID()), true, http.StatusNotFound)
		return
	}
	event := client.Event{
		Link:  s.topicEventLink(t.ID(), id),
		ID:    id,
		State: s.convertEventState(state),
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(event, true))
}

func (s *Service) handleListTopicHandlers(t *Topic, w http.ResponseWriter, r *http.Request) {

}

func (s *Service) handleTopicHandler(t *Topic, w http.ResponseWriter, r *http.Request) {}

func (s *Service) handleListHandlers(w http.ResponseWriter, r *http.Request) {
	pattern := r.URL.Query().Get("pattern")
	if err := validatePattern(pattern); err != nil {
		httpd.HttpError(w, fmt.Sprint("invalid pattern: ", err.Error()), true, http.StatusBadRequest)
		return
	}
	handlers := client.Handlers{
		Link:     client.Link{Relation: client.Self, Href: r.URL.String()},
		Handlers: s.Handlers(pattern),
	}

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(handlers, true))
}

func (s *Service) handleCreateHandler(w http.ResponseWriter, r *http.Request) {
	handlerSpec := HandlerSpec{}
	err := json.NewDecoder(r.Body).Decode(&handlerSpec)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("invalid handler json: ", err.Error()), true, http.StatusBadRequest)
		return
	}

	err = s.RegisterHandlerSpec(handlerSpec)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to create handler: ", err.Error()), true, http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) EventState(topic, event string) (EventState, bool) {
	t, ok := s.system.Topic(topic)
	if !ok {
		return EventState{}, false
	}
	return t.EventState(event)
}

func (s *Service) Collect(event Event) error {
	return s.system.Collect(event)
}

func (s *Service) DeleteTopic(topic string) {
	s.system.DeleteTopic(topic)
}

func (s *Service) RegisterHandler(topics []string, h Handler) {
	s.system.RegisterHandler(topics, h)
}
func (s *Service) DeregisterHandler(topics []string, h Handler) {
	s.system.DeregisterHandler(topics, h)
}

func (s *Service) RegisterHandlerSpec(spec HandlerSpec) error {
	s.mu.RLock()
	_, ok := s.handlerSpecs[spec.ID]
	s.mu.RUnlock()
	if ok {
		return fmt.Errorf("cannot register handler, handler with ID %q already exists", spec.ID)
	}
	h, err := s.createHandlerFromSpec(spec)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.handlerSpecs[spec.ID] = spec
	s.handlers[spec.ID] = h
	s.mu.Unlock()

	s.system.RegisterHandler(spec.Topics, h)
	return nil
}

func (s *Service) DeregisterHandlerSpec(spec HandlerSpec) {
	s.mu.Lock()
	h := s.handlers[spec.ID]
	delete(s.handlerSpecs, spec.ID)
	delete(s.handlers, spec.ID)
	s.mu.Unlock()

	if h != nil {
		s.system.DeregisterHandler(spec.Topics, h)
	}
}

// TopicStatus returns the max alert level for each topic matching 'pattern', not returning
// any topics with max alert levels less severe than 'minLevel'
func (s *Service) TopicStatus(pattern string, minLevel Level) []client.Topic {
	statuses := s.system.TopicStatus(pattern, minLevel)
	topics := make([]client.Topic, 0, len(statuses))
	for topic, level := range statuses {
		topics = append(topics, s.createClientTopic(topic, level))
	}
	return topics
}

// TopicStatusDetails is similar to TopicStatus, but will additionally return
// at least 'minLevel' severity
func (s *Service) TopicStatusEvents(pattern string, minLevel Level) map[string]map[string]EventState {
	return s.system.TopicStatusEvents(pattern, minLevel)
}

func (s *Service) Handlers(pattern string) []client.Handler {
	s.mu.RLock()
	defer s.mu.RUnlock()

	handlers := make([]client.Handler, 0, len(s.handlerSpecs))
	for id, spec := range s.handlerSpecs {
		actions := make([]client.HandlerAction, 0, len(spec.Actions))
		for _, actionSpec := range spec.Actions {
			action := make(client.HandlerAction, 1)
			action[actionSpec.Kind] = actionSpec.Options
			actions = append(actions, action)
		}
		handlers = append(handlers, client.Handler{
			Link:    s.handlerLink(id),
			ID:      id,
			Topics:  spec.Topics,
			Actions: actions,
		})
	}
	return handlers
}

func (s *Service) createHandlerFromSpec(spec HandlerSpec) (Handler, error) {
	if 0 == len(spec.Actions) {
		return nil, errors.New("invalid handler spec, must have at least one action")
	}

	// Create actions chained together in a singly linked list
	var prev, first HandlerAction
	for _, actionSpec := range spec.Actions {
		curr, err := s.createHandlerActionFromSpec(actionSpec)
		if err != nil {
			return nil, err
		}
		if first == nil {
			// Keep first action
			first = curr
		}
		if prev != nil {
			// Link previous action to current action
			prev.SetNext(curr)
		}
		prev = curr
	}

	// set a noopHandler for the last action
	prev.SetNext(noopHandler{})

	return first, nil
}

func (s *Service) createHandlerActionFromSpec(spec HandlerActionSpec) (ha HandlerAction, err error) {
	var h Handler
	switch spec.Kind {
	case "slack":
		h, err = s.Slack.HandlerFromOptions(spec.Options, s.logger)
		ha = newPassThroughHandler(h)
	//case "smtp":
	//	h = newPassThroughHandler(s.SMTPService.Handler(spec.Options,s.logger))
	default:
		err = fmt.Errorf("unsupported action kind %q", spec.Kind)
	}
	return
}

// PassThroughHandler implements HandlerAction and passes through all events to the next handler.
type passThroughHandler struct {
	h    Handler
	next Handler
}

func newPassThroughHandler(h Handler) *passThroughHandler {
	return &passThroughHandler{
		h: h,
	}
}

func (h *passThroughHandler) Handle(event Event) {
	h.h.Handle(event)
	h.next.Handle(event)
}

func (h *passThroughHandler) SetNext(next Handler) {
	h.next = next
}

// NoopHandler implements Handler and does nothing with the event
type noopHandler struct{}

func (h noopHandler) Handle(event Event) {}
