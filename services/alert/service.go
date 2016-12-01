package alert

import (
	"fmt"
	"log"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"

	client "github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/services/httpd"
)

const (
	// eventBufferSize is the number of events to buffer to each handler per topic.
	eventBufferSize = 100

	alertsPath         = "/alerts"
	alertsPathAnchored = "/alerts/"

	topicsPath             = alertsPath + "/topics"
	topicsPathAnchored     = alertsPath + "/topics/"
	topicsBasePath         = httpd.BasePath + topicsPath
	topicsBasePathAnchored = httpd.BasePath + topicsPathAnchored

	handlersPath         = alertsPath + "/handlers"
	handlersPathAnchored = alertsPath + "/handlers/"

	topicEventsPath   = "events"
	topicHandlersPath = "handlers"

	eventsRelation   = "events"
	handlersRelation = "handlers"
)

type Service struct {
	mu sync.RWMutex

	handlers map[string]HandlerConfig

	topics map[string]*Topic

	routes       []httpd.Route
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}

	logger *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		handlers: make(map[string]HandlerConfig),
		topics:   make(map[string]*Topic),
		logger:   l,
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
		//{
		//	Method:      "GET",
		//	Pattern:     handlersPath,
		//	HandlerFunc: s.handleListHandlers,
		//},
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
	for topic, t := range s.topics {
		t.Close()
		delete(s.topics, topic)
	}
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

const eventsPattern = "*/events"
const eventPattern = "*/events/*"
const handlersPattern = "*/handlers"
const handlerPattern = "*/handlers/*"

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
	s.mu.RLock()
	t, ok := s.topics[id]
	s.mu.RUnlock()
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

func (s *Service) convertTopic(t *Topic) client.Topic {
	return client.Topic{
		ID:           t.ID(),
		Link:         s.topicLink(t.ID()),
		Level:        t.MaxLevel().String(),
		EventsLink:   s.topicEventsLink(t.ID(), eventsRelation),
		HandlersLink: s.topicHandlersLink(t.ID(), handlersRelation),
	}
}

func (s *Service) handleTopic(t *Topic, w http.ResponseWriter, r *http.Request) {
	topic := s.convertTopic(t)

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
	res := client.Events{
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

func (s *Service) handleListTopicHandlers(t *Topic, w http.ResponseWriter, r *http.Request) {}
func (s *Service) handleTopicHandler(t *Topic, w http.ResponseWriter, r *http.Request)      {}

func (s *Service) Collect(event Event) error {
	s.mu.RLock()
	topic := s.topics[event.Topic]
	s.mu.RUnlock()

	if topic == nil {
		// Create the empty topic
		s.mu.Lock()
		// Check again if the topic was created, now that we have the write lock
		topic = s.topics[event.Topic]
		if topic == nil {
			topic = newTopic(event.Topic)
			s.topics[event.Topic] = topic
		}
		s.mu.Unlock()
	}

	return topic.Handle(event)
}

func (s *Service) DeleteTopic(topic string) {
	s.mu.Lock()
	t := s.topics[topic]
	delete(s.topics, topic)
	s.mu.Unlock()
	if t != nil {
		t.Close()
	}
}

func (s *Service) RegisterHandler(topics []string, h Handler) {
	if len(topics) == 0 || h == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, topic := range topics {
		if _, ok := s.topics[topic]; !ok {
			s.topics[topic] = newTopic(topic)
		}
		s.topics[topic].AddHandler(h)
	}
}

func (s *Service) DeregisterHandler(topics []string, h Handler) {
	if len(topics) == 0 || h == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, topic := range topics {
		s.topics[topic].RemoveHandler(h)
	}
}

// TopicStatus returns the max alert level for each topic matching 'pattern', not returning
// any topics with max alert levels less severe than 'minLevel'
func (s *Service) TopicStatus(pattern string, minLevel Level) []client.Topic {
	s.mu.RLock()
	res := make([]client.Topic, 0, len(s.topics))
	for _, topic := range s.topics {
		level := topic.MaxLevel()
		if level >= minLevel && match(pattern, topic.ID()) {
			res = append(res, s.convertTopic(topic))
		}
	}
	s.mu.RUnlock()
	return res
}

// TopicStatusDetails is similar to TopicStatus, but will additionally return
// at least 'minLevel' severity
func (s *Service) TopicStatusEvents(pattern string, minLevel Level) map[string]map[string]EventState {
	s.mu.RLock()
	topics := make([]*Topic, 0, len(s.topics))
	for _, topic := range s.topics {
		if topic.MaxLevel() >= minLevel && match(pattern, topic.name) {
			topics = append(topics, topic)
		}
	}
	s.mu.RUnlock()

	res := make(map[string]map[string]EventState, len(topics))

	for _, topic := range topics {
		res[topic.ID()] = topic.Events(minLevel)
	}

	return res
}

func match(pattern, name string) bool {
	if pattern == "" {
		return true
	}
	matched, _ := path.Match(pattern, name)
	return matched
}

type Topic struct {
	name string

	mu sync.RWMutex

	events map[string]*EventState
	sorted []*EventState

	handlers []*handler
}

func newTopic(name string) *Topic {
	return &Topic{
		name:   name,
		events: make(map[string]*EventState),
	}
}
func (t *Topic) ID() string {
	return t.name
}

func (t *Topic) MaxLevel() Level {
	level := OK
	t.mu.RLock()
	if len(t.sorted) > 0 {
		level = t.sorted[0].Level
	}
	t.mu.RUnlock()
	return level
}

func (t *Topic) AddHandler(h Handler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, cur := range t.handlers {
		if cur.Equal(h) {
			return
		}
	}
	hdlr := newHandler(h)
	t.handlers = append(t.handlers, hdlr)
}

func (t *Topic) RemoveHandler(h Handler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := 0; i < len(t.handlers); i++ {
		if t.handlers[i].Equal(h) {
			// Close handler
			t.handlers[i].Close()
			if i < len(t.handlers)-1 {
				t.handlers[i] = t.handlers[len(t.handlers)-1]
			}
			t.handlers = t.handlers[:len(t.handlers)-1]
			break
		}
	}
}

func (t *Topic) Events(minLevel Level) map[string]EventState {
	t.mu.RLock()
	events := make(map[string]EventState, len(t.sorted))
	for _, e := range t.sorted {
		if e.Level < minLevel {
			break
		}
		events[e.ID] = *e
	}
	t.mu.RUnlock()
	return events
}

func (t *Topic) EventState(event string) (EventState, bool) {
	t.mu.RLock()
	state, ok := t.events[event]
	t.mu.RUnlock()
	if ok {
		return *state, true
	}
	return EventState{}, false
}

func (t *Topic) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Close all handlers
	for _, h := range t.handlers {
		h.Close()
	}
	t.handlers = nil
}

func (t *Topic) Handle(event Event) error {
	t.updateEvent(event.State)
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Handle event
	var errs multiError
	for _, h := range t.handlers {
		err := h.Handle(event)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return errs
	}
	return nil
}

// updateEvent will store the latest state for the given ID.
func (t *Topic) updateEvent(state EventState) {
	var needSort bool
	t.mu.Lock()
	defer t.mu.Unlock()
	cur := t.events[state.ID]
	if cur == nil {
		needSort = true
		cur = new(EventState)
		t.events[state.ID] = cur
		t.sorted = append(t.sorted, cur)
	}
	needSort = needSort || cur.Level != state.Level

	*cur = state

	if needSort {
		sort.Sort(sortedStates(t.sorted))
	}
}

type sortedStates []*EventState

func (e sortedStates) Len() int          { return len(e) }
func (e sortedStates) Swap(i int, j int) { e[i], e[j] = e[j], e[i] }
func (e sortedStates) Less(i int, j int) bool {
	if e[i].Level > e[j].Level {
		return true
	}
	return e[i].ID < e[j].ID
}

// handler wraps a Handler implementation in order to provide buffering and non-blocking event handling.
type handler struct {
	h        Handler
	events   chan Event
	aborting chan struct{}
	wg       sync.WaitGroup
}

func newHandler(h Handler) *handler {
	hdlr := &handler{
		h:        h,
		events:   make(chan Event, eventBufferSize),
		aborting: make(chan struct{}),
	}
	hdlr.wg.Add(1)
	go func() {
		defer hdlr.wg.Done()
		hdlr.run()
	}()
	return hdlr
}

func (h *handler) Equal(o Handler) (b bool) {
	defer func() {
		// Recover in case the interface concrete type is not a comparable type.
		r := recover()
		if r != nil {
			b = false
		}
	}()
	b = h.h == o
	return
}

func (h *handler) Close() {
	close(h.events)
	h.wg.Wait()
}

func (h *handler) Abort() {
	close(h.aborting)
	h.wg.Wait()
}

func (h *handler) Handle(event Event) error {
	select {
	case h.events <- event:
		return nil
	default:
		return fmt.Errorf("failed to deliver event %q to handler", event.State.ID)
	}
}

func (h *handler) run() {
	for {
		select {
		case event, ok := <-h.events:
			if !ok {
				return
			}
			h.h.Handle(event)
		case <-h.aborting:
			return
		}
	}
}

// multiError is a list of errors.
type multiError []error

func (e multiError) Error() string {
	if len(e) == 1 {
		return e[0].Error()
	}
	msg := "multiple errors:"
	for _, err := range e {
		msg += "\n" + err.Error()
	}
	return msg
}
