package alert

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"strings"
	"sync"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/influxdata/kapacitor/alert"
	client "github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/services/alerta"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

const (
	alertsPath         = "/alerts"
	alertsPathAnchored = "/alerts/"

	topicsPath             = alertsPath + "/topics"
	topicsPathAnchored     = alertsPath + "/topics/"
	topicsBasePath         = httpd.BasePath + topicsPath
	topicsBasePathAnchored = httpd.BasePath + topicsPathAnchored

	handlersPath             = alertsPath + "/handlers"
	handlersPathAnchored     = alertsPath + "/handlers/"
	handlersBasePath         = httpd.BasePath + handlersPath
	handlersBasePathAnchored = httpd.BasePath + handlersPathAnchored

	topicEventsPath   = "events"
	topicHandlersPath = "handlers"

	eventsPattern   = "*/" + topicEventsPath
	eventPattern    = "*/" + topicEventsPath + "/*"
	handlersPattern = "*/" + topicHandlersPath

	eventsRelation   = "events"
	handlersRelation = "handlers"
)

type handler struct {
	Spec    HandlerSpec
	Handler alert.Handler
}

type handlerAction interface {
	Handle(event alert.Event)
	SetNext(h alert.Handler)
}

type Service struct {
	mu sync.RWMutex

	specsDAO  HandlerSpecDAO
	topicsDAO TopicStateDAO

	handlers map[string]handler

	topics *alert.Topics

	routes       []httpd.Route
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
	StorageService interface {
		Store(namespace string) storage.Interface
	}

	Commander command.Commander

	logger *log.Logger

	AlertaService interface {
		DefaultHandlerConfig() alerta.HandlerConfig
		Handler(alerta.HandlerConfig, *log.Logger) (alert.Handler, error)
	}
	HipChatService interface {
		Handler(hipchat.HandlerConfig, *log.Logger) alert.Handler
	}
	OpsGenieService interface {
		Handler(opsgenie.HandlerConfig, *log.Logger) alert.Handler
	}
	PagerDutyService interface {
		Handler(pagerduty.HandlerConfig, *log.Logger) alert.Handler
	}
	SensuService interface {
		Handler(*log.Logger) alert.Handler
	}
	SlackService interface {
		Handler(slack.HandlerConfig, *log.Logger) alert.Handler
	}
	SMTPService interface {
		Handler(smtp.HandlerConfig, *log.Logger) alert.Handler
	}
	TalkService interface {
		Handler(*log.Logger) alert.Handler
	}
	TelegramService interface {
		Handler(telegram.HandlerConfig, *log.Logger) alert.Handler
	}
	VictorOpsService interface {
		Handler(victorops.HandlerConfig, *log.Logger) alert.Handler
	}
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		handlers: make(map[string]handler),
		topics:   alert.NewTopics(l),
		logger:   l,
	}
	return s
}

// The storage namespace for all task data.
const alertNamespace = "alert_store"

func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create DAO
	store := s.StorageService.Store(alertNamespace)
	specsDAO, err := newHandlerSpecKV(store)
	if err != nil {
		return err
	}
	s.specsDAO = specsDAO
	topicsDAO, err := newTopicStateKV(store)
	if err != nil {
		return err
	}
	s.topicsDAO = topicsDAO

	// Load saved handlers
	if err := s.loadSavedHandlerSpecs(); err != nil {
		return err
	}

	// Load saved topic state
	if err := s.loadSavedTopicStates(); err != nil {
		return err
	}

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
			Method:      "DELETE",
			Pattern:     topicsPathAnchored,
			HandlerFunc: s.handleDeleteTopic,
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
		{
			// Satisfy CORS checks.
			Method:      "OPTIONS",
			Pattern:     handlersPathAnchored,
			HandlerFunc: httpd.ServeOptions,
		},
		{
			Method:      "PATCH",
			Pattern:     handlersPathAnchored,
			HandlerFunc: s.handlePatchHandler,
		},
		{
			Method:      "PUT",
			Pattern:     handlersPathAnchored,
			HandlerFunc: s.handlePutHandler,
		},
		{
			Method:      "DELETE",
			Pattern:     handlersPathAnchored,
			HandlerFunc: s.handleDeleteHandler,
		},
		{
			Method:      "GET",
			Pattern:     handlersPathAnchored,
			HandlerFunc: s.handleGetHandler,
		},
	}

	return s.HTTPDService.AddRoutes(s.routes)
}

func (s *Service) loadSavedHandlerSpecs() error {
	offset := 0
	limit := 100
	for {
		specs, err := s.specsDAO.List("", offset, limit)
		if err != nil {
			return err
		}

		for _, spec := range specs {
			if err := s.loadHandlerSpec(spec); err != nil {
				s.logger.Println("E! failed to load handler on startup", err)
			}
		}

		offset += limit
		if len(specs) != limit {
			break
		}
	}
	return nil
}

func (s *Service) loadSavedTopicStates() error {
	offset := 0
	limit := 100
	for {
		topicStates, err := s.topicsDAO.List("", offset, limit)
		if err != nil {
			return err
		}

		for _, ts := range topicStates {
			s.topics.RestoreTopic(ts.Topic, s.convertEventStatesToAlert(ts.EventStates))
		}

		offset += limit
		if len(topicStates) != limit {
			break
		}
	}
	return nil
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics.Close()
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
	minLevel, err := alert.ParseLevel(minLevelStr)
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

func (s *Service) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, topicsBasePathAnchored)
	id := s.topicIDFromPath(p)
	if err := s.DeleteTopic(id); err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to delete topic %q: %v", id, err), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleRouteTopic(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, topicsBasePathAnchored)
	id := s.topicIDFromPath(p)
	t, ok := s.topics.Topic(id)
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

func (s *Service) createClientTopic(topic string, level alert.Level) client.Topic {
	return client.Topic{
		ID:           topic,
		Link:         s.topicLink(topic),
		Level:        level.String(),
		EventsLink:   s.topicEventsLink(topic, eventsRelation),
		HandlersLink: s.topicHandlersLink(topic, handlersRelation),
	}
}

func (s *Service) handleTopic(t *alert.Topic, w http.ResponseWriter, r *http.Request) {
	topic := s.createClientTopic(t.ID(), t.MaxLevel())

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(topic, true))
}

func (s *Service) convertEventStatesFromAlert(states map[string]alert.EventState) map[string]EventState {
	newStates := make(map[string]EventState, len(states))
	for id, state := range states {
		newStates[id] = s.convertEventStateFromAlert(state)
	}
	return newStates
}

func (s *Service) convertEventStatesToAlert(states map[string]EventState) map[string]alert.EventState {
	newStates := make(map[string]alert.EventState, len(states))
	for id, state := range states {
		newStates[id] = s.convertEventStateToAlert(id, state)
	}
	return newStates
}

func (s *Service) convertEventStateFromAlert(state alert.EventState) EventState {
	return EventState{
		Message:  state.Message,
		Details:  state.Details,
		Time:     state.Time,
		Duration: state.Duration,
		Level:    state.Level,
	}
}

func (s *Service) convertEventStateToAlert(id string, state EventState) alert.EventState {
	return alert.EventState{
		ID:       id,
		Message:  state.Message,
		Details:  state.Details,
		Time:     state.Time,
		Duration: state.Duration,
		Level:    state.Level,
	}
}

func (s *Service) convertEventStateToClient(state alert.EventState) client.EventState {
	return client.EventState{
		Message:  state.Message,
		Details:  state.Details,
		Time:     state.Time,
		Duration: state.Duration,
		Level:    state.Level.String(),
	}
}

func (s *Service) convertHandlerSpec(spec HandlerSpec) client.Handler {
	actions := make([]client.HandlerAction, 0, len(spec.Actions))
	for _, actionSpec := range spec.Actions {
		action := client.HandlerAction{
			Kind:    actionSpec.Kind,
			Options: actionSpec.Options,
		}
		actions = append(actions, action)
	}
	return client.Handler{
		Link:    s.handlerLink(spec.ID),
		ID:      spec.ID,
		Topics:  spec.Topics,
		Actions: actions,
	}
}

func (s *Service) handleListTopicEvents(t *alert.Topic, w http.ResponseWriter, r *http.Request) {
	minLevelStr := r.URL.Query().Get("min-level")
	minLevel, err := alert.ParseLevel(minLevelStr)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	events := t.EventStates(minLevel)
	res := client.TopicEvents{
		Link:   s.topicEventsLink(t.ID(), client.Self),
		Topic:  t.ID(),
		Events: make([]client.Event, 0, len(events)),
	}
	for id, state := range events {
		res.Events = append(res.Events, client.Event{
			Link:  s.topicEventLink(t.ID(), id),
			ID:    id,
			State: s.convertEventStateToClient(state),
		})
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(res, true))
}

func (s *Service) handleTopicEvent(t *alert.Topic, w http.ResponseWriter, r *http.Request) {
	id := path.Base(r.URL.Path)
	state, ok := t.EventState(id)
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("event %q does not exist for topic %q", id, t.ID()), true, http.StatusNotFound)
		return
	}
	event := client.Event{
		Link:  s.topicEventLink(t.ID(), id),
		ID:    id,
		State: s.convertEventStateToClient(state),
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(event, true))
}

func (s *Service) handleListTopicHandlers(t *alert.Topic, w http.ResponseWriter, r *http.Request) {
	var handlers []client.Handler
	for _, h := range s.handlers {
		match := false
		for _, topic := range h.Spec.Topics {
			if topic == t.ID() {
				match = true
				break
			}
		}
		if match {
			handlers = append(handlers, s.convertHandlerSpec(h.Spec))
		}
	}
	th := client.TopicHandlers{
		Link:     s.topicHandlersLink(t.ID(), client.Self),
		Topic:    t.ID(),
		Handlers: handlers,
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(th, true))
}

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
		httpd.HttpError(w, fmt.Sprint("failed to create handler: ", err.Error()), true, http.StatusInternalServerError)
		return
	}

	h := s.convertHandlerSpec(handlerSpec)
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(h, true))
}

func (s *Service) handlePatchHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, handlersBasePathAnchored)
	s.mu.RLock()
	h, ok := s.handlers[id]
	s.mu.RUnlock()
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("unknown handler: %q", id), true, http.StatusNotFound)
		return
	}

	patchBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to read request body: ", err.Error()), true, http.StatusInternalServerError)
		return
	}
	patch, err := jsonpatch.DecodePatch(patchBytes)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("invalid patch json: ", err.Error()), true, http.StatusBadRequest)
		return
	}
	specBytes, err := json.Marshal(h.Spec)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to marshal JSON: ", err.Error()), true, http.StatusInternalServerError)
		return
	}
	newBytes, err := patch.Apply(specBytes)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to apply patch: ", err.Error()), true, http.StatusBadRequest)
		return
	}
	newSpec := HandlerSpec{}
	if err := json.Unmarshal(newBytes, &newSpec); err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to unmarshal patched json: ", err.Error()), true, http.StatusInternalServerError)
		return
	}

	if err := s.ReplaceHandlerSpec(h.Spec, newSpec); err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to update handler: ", err.Error()), true, http.StatusInternalServerError)
		return
	}

	ch := s.convertHandlerSpec(newSpec)

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(ch, true))
}

func (s *Service) handlePutHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, handlersBasePathAnchored)
	s.mu.RLock()
	h, ok := s.handlers[id]
	s.mu.RUnlock()
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("unknown handler: %q", id), true, http.StatusNotFound)
		return
	}

	newSpec := HandlerSpec{}
	err := json.NewDecoder(r.Body).Decode(&newSpec)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to unmar JSON: ", err.Error()), true, http.StatusBadRequest)
		return
	}

	if err := s.ReplaceHandlerSpec(h.Spec, newSpec); err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to update handler: ", err.Error()), true, http.StatusInternalServerError)
		return
	}

	ch := s.convertHandlerSpec(newSpec)

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(ch, true))
}

func (s *Service) handleDeleteHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, handlersBasePathAnchored)
	if err := s.DeregisterHandlerSpec(id); err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to delete handler: ", err.Error()), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleGetHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, handlersBasePathAnchored)
	s.mu.RLock()
	h, ok := s.handlers[id]
	s.mu.RUnlock()
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("unknown handler: %q", id), true, http.StatusNotFound)
		return
	}

	ch := s.convertHandlerSpec(h.Spec)

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(ch, true))
}

func (s *Service) EventState(topic, event string) (alert.EventState, bool) {
	t, ok := s.topics.Topic(topic)
	if !ok {
		return alert.EventState{}, false
	}
	return t.EventState(event)
}

func (s *Service) Collect(event alert.Event) error {
	err := s.topics.Collect(event)
	if err != nil {
		return err
	}
	t, ok := s.topics.Topic(event.Topic)
	if !ok {
		// Topic was deleted, nothing to do
		return nil
	}

	ts := TopicState{
		Topic:       event.Topic,
		EventStates: s.convertEventStatesFromAlert(t.EventStates(alert.OK)),
	}
	return s.topicsDAO.Put(ts)
}

func (s *Service) DeleteTopic(topic string) error {
	s.topics.DeleteTopic(topic)
	return s.topicsDAO.Delete(topic)
}

func (s *Service) RegisterHandler(topics []string, h alert.Handler) {
	s.topics.RegisterHandler(topics, h)
}
func (s *Service) DeregisterHandler(topics []string, h alert.Handler) {
	s.topics.DeregisterHandler(topics, h)
}

// loadHandlerSpec initializes a spec that already exists.
// Caller must have the write lock.
func (s *Service) loadHandlerSpec(spec HandlerSpec) error {
	h, err := s.createHandlerFromSpec(spec)
	if err != nil {
		return err
	}

	s.handlers[spec.ID] = h

	s.topics.RegisterHandler(spec.Topics, h.Handler)
	return nil
}

func (s *Service) RegisterHandlerSpec(spec HandlerSpec) error {
	s.mu.RLock()
	_, ok := s.handlers[spec.ID]
	s.mu.RUnlock()
	if ok {
		return fmt.Errorf("cannot register handler, handler with ID %q already exists", spec.ID)
	}

	h, err := s.createHandlerFromSpec(spec)
	if err != nil {
		return err
	}

	// Persist handler spec
	if err := s.specsDAO.Create(spec); err != nil {
		return err
	}

	s.mu.Lock()
	s.handlers[spec.ID] = h
	s.mu.Unlock()

	s.topics.RegisterHandler(spec.Topics, h.Handler)
	return nil
}

func (s *Service) DeregisterHandlerSpec(id string) error {
	s.mu.RLock()
	h, ok := s.handlers[id]
	s.mu.RUnlock()

	if ok {
		// Delete handler spec
		if err := s.specsDAO.Delete(id); err != nil {
			return err
		}
		s.topics.DeregisterHandler(h.Spec.Topics, h.Handler)

		s.mu.Lock()
		delete(s.handlers, id)
		s.mu.Unlock()
	}
	return nil
}

func (s *Service) ReplaceHandlerSpec(oldSpec, newSpec HandlerSpec) error {
	newH, err := s.createHandlerFromSpec(newSpec)
	if err != nil {
		return err
	}

	s.mu.RLock()
	oldH := s.handlers[oldSpec.ID]
	s.mu.RUnlock()

	// Persist new handler specs
	if newSpec.ID == oldSpec.ID {
		if err := s.specsDAO.Replace(newSpec); err != nil {
			return err
		}
	} else {
		if err := s.specsDAO.Create(newSpec); err != nil {
			return err
		}
		if err := s.specsDAO.Delete(oldSpec.ID); err != nil {
			return err
		}
	}

	s.mu.Lock()
	delete(s.handlers, oldSpec.ID)
	s.handlers[newSpec.ID] = newH
	s.mu.Unlock()

	s.topics.ReplaceHandler(oldSpec.Topics, newSpec.Topics, oldH.Handler, newH.Handler)
	return nil
}

// TopicStatus returns the max alert level for each topic matching 'pattern', not returning
// any topics with max alert levels less severe than 'minLevel'
func (s *Service) TopicStatus(pattern string, minLevel alert.Level) []client.Topic {
	statuses := s.topics.TopicStatus(pattern, minLevel)
	topics := make([]client.Topic, 0, len(statuses))
	for topic, level := range statuses {
		topics = append(topics, s.createClientTopic(topic, level))
	}
	return topics
}

// TopicStatusDetails is similar to TopicStatus, but will additionally return
// at least 'minLevel' severity
func (s *Service) TopicStatusEvents(pattern string, minLevel alert.Level) map[string]map[string]alert.EventState {
	return s.topics.TopicStatusEvents(pattern, minLevel)
}

func (s *Service) Handlers(pattern string) []client.Handler {
	s.mu.RLock()
	defer s.mu.RUnlock()

	handlers := make([]client.Handler, 0, len(s.handlers))
	for id, h := range s.handlers {
		if match(pattern, id) {
			handlers = append(handlers, s.convertHandlerSpec(h.Spec))
		}
	}
	return handlers
}
func match(pattern, id string) bool {
	if pattern == "" {
		return true
	}
	matched, _ := path.Match(pattern, id)
	return matched
}

func (s *Service) createHandlerFromSpec(spec HandlerSpec) (handler, error) {
	if 0 == len(spec.Actions) {
		return handler{}, errors.New("invalid handler spec, must have at least one action")
	}

	// Create actions chained together in a singly linked list
	var prev, first handlerAction
	for _, actionSpec := range spec.Actions {
		curr, err := s.createHandlerActionFromSpec(actionSpec)
		if err != nil {
			return handler{}, err
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

	return handler{Spec: spec, Handler: first}, nil
}

func decodeOptions(options map[string]interface{}, c interface{}) error {
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		ErrorUnused: true,
		Result:      c,
	})
	if err != nil {
		return errors.Wrap(err, "failed to initialize mapstructure decoder")
	}
	if err := dec.Decode(options); err != nil {
		return errors.Wrapf(err, "failed to decode options into %T", c)
	}
	return nil
}

func (s *Service) createHandlerActionFromSpec(spec HandlerActionSpec) (ha handlerAction, err error) {
	switch spec.Kind {
	case "alerta":
		c := s.AlertaService.DefaultHandlerConfig()
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h, err := s.AlertaService.Handler(c, s.logger)
		if err != nil {
			return nil, err
		}
		ha = newPassThroughHandler(h)
	case "exec":
		c := alert.ExecHandlerConfig{
			Commander: s.Commander,
		}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := alert.NewExecHandler(c, s.logger)
		ha = newPassThroughHandler(h)
	case "hipchat":
		c := hipchat.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := s.HipChatService.Handler(c, s.logger)
		ha = newPassThroughHandler(h)
	case "log":
		c := alert.DefaultLogHandlerConfig()
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h, err := alert.NewLogHandler(c, s.logger)
		if err != nil {
			return nil, err
		}
		ha = newPassThroughHandler(h)
	case "opsgenie":
		c := opsgenie.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := s.OpsGenieService.Handler(c, s.logger)
		ha = newPassThroughHandler(h)
	case "pagerduty":
		c := pagerduty.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := s.PagerDutyService.Handler(c, s.logger)
		ha = newPassThroughHandler(h)
	case "post":
		c := alert.PostHandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := alert.NewPostHandler(c, s.logger)
		ha = newPassThroughHandler(h)
	case "sensu":
		h := s.SensuService.Handler(s.logger)
		ha = newPassThroughHandler(h)
	case "slack":
		c := slack.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := s.SlackService.Handler(c, s.logger)
		ha = newPassThroughHandler(h)
	case "smtp":
		c := smtp.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := s.SMTPService.Handler(c, s.logger)
		ha = newPassThroughHandler(h)
	case "talk":
		h := s.TalkService.Handler(s.logger)
		ha = newPassThroughHandler(h)
	case "tcp":
		c := alert.TCPHandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := alert.NewTCPHandler(c, s.logger)
		ha = newPassThroughHandler(h)
	case "telegram":
		c := telegram.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := s.TelegramService.Handler(c, s.logger)
		ha = newPassThroughHandler(h)
	case "victorops":
		c := victorops.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := s.VictorOpsService.Handler(c, s.logger)
		ha = newPassThroughHandler(h)
	default:
		err = fmt.Errorf("unsupported action kind %q", spec.Kind)
		log.Println("error", err)
	}
	return
}

// PassThroughHandler implements HandlerAction and passes through all events to the next handler.
type passThroughHandler struct {
	h    alert.Handler
	next alert.Handler
}

func newPassThroughHandler(h alert.Handler) *passThroughHandler {
	return &passThroughHandler{
		h: h,
	}
}

func (h *passThroughHandler) Handle(event alert.Event) {
	h.h.Handle(event)
	h.next.Handle(event)
}

func (h *passThroughHandler) SetNext(next alert.Handler) {
	h.next = next
}

// NoopHandler implements Handler and does nothing with the event
type noopHandler struct{}

func (h noopHandler) Handle(event alert.Event) {}
