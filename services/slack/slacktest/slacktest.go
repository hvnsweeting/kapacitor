package slacktest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
)

type Server struct {
	ts       *httptest.Server
	URL      string
	requests chan Request
	Requests <-chan Request
	closed   bool
}

func NewServer(count int) *Server {
	requests := make(chan Request, count)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sr := Request{
			URL: r.URL.String(),
		}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&sr.PostData)
		requests <- sr
	}))
	return &Server{
		ts:       ts,
		URL:      ts.URL,
		requests: requests,
		Requests: requests,
	}
}

func (s *Server) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.ts.Close()
	close(s.requests)
}

type Request struct {
	URL      string
	PostData PostData
}

type PostData struct {
	Channel     string       `json:"channel"`
	Username    string       `json:"username"`
	Text        string       `json:"text"`
	Attachments []Attachment `json:"attachments"`
}

type Attachment struct {
	Fallback string `json:"fallback"`
	Color    string `json:"color"`
	Text     string `json:"text"`
}
