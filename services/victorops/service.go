package victorops

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path"
	"sync/atomic"
	"time"

	"github.com/influxdata/kapacitor"
	"github.com/pkg/errors"
)

type Service struct {
	configValue atomic.Value
	logger      *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		logger: l,
	}
	s.configValue.Store(c)
	return s
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) config() Config {
	return s.configValue.Load().(Config)
}

func (s *Service) Update(newConfig []interface{}) error {
	if l := len(newConfig); l != 1 {
		return fmt.Errorf("expected only one new config object, got %d", l)
	}
	if c, ok := newConfig[0].(Config); !ok {
		return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfig[0])
	} else {
		s.configValue.Store(c)
	}
	return nil
}

func (s *Service) Global() bool {
	c := s.config()
	return c.Global
}

type testOptions struct {
	RoutingKey  string `json:"routingKey"`
	MessageType string `json:"messageType"`
	Message     string `json:"message"`
	EntityID    string `json:"entityID"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		RoutingKey:  c.RoutingKey,
		MessageType: "CRITICAL",
		Message:     "test victorops message",
		EntityID:    "testEntityID",
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(
		o.RoutingKey,
		o.MessageType,
		o.Message,
		o.EntityID,
		time.Now(),
		nil,
	)
}

func (s *Service) Alert(routingKey, messageType, message, entityID string, t time.Time, details interface{}) error {
	url, post, err := s.preparePost(routingKey, messageType, message, entityID, t, details)
	if err != nil {
		return err
	}

	resp, err := http.Post(url, "application/json", post)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return errors.New("URL or API key not found: 404")
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		type response struct {
			Message string `json:"message"`
		}
		r := &response{Message: fmt.Sprintf("failed to understand VictorOps response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Message)
	}
	return nil
}

func (s *Service) preparePost(routingKey, messageType, message, entityID string, t time.Time, details interface{}) (string, io.Reader, error) {
	c := s.config()
	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}

	voData := make(map[string]interface{})
	voData["message_type"] = messageType
	voData["entity_id"] = entityID
	voData["state_message"] = message
	voData["timestamp"] = t.Unix()
	voData["monitoring_tool"] = kapacitor.Product
	if details != nil {
		b, err := json.Marshal(details)
		if err != nil {
			return "", nil, err
		}
		voData["data"] = string(b)
	}

	if routingKey == "" {
		routingKey = c.RoutingKey
	}

	// Post data to VO
	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(voData)
	if err != nil {
		return "", nil, err
	}
	u, err := url.Parse(c.URL)
	if err != nil {
		return "", nil, errors.Wrap(err, "invalid URL")
	}
	u.Path = path.Join(u.Path, c.APIKey, routingKey)
	return u.String(), &post, nil
}
