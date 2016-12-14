package alertatest

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
		ar := Request{
			URL:           r.URL.String(),
			Authorization: r.Header.Get("Authorization"),
		}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&ar.PostData)
		requests <- ar
		w.WriteHeader(http.StatusCreated)
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
	URL           string
	Authorization string
	PostData      PostData
}

type PostData struct {
	Resource    string   `json:"resource"`
	Event       string   `json:"event"`
	Group       string   `json:"group"`
	Environment string   `json:"environment"`
	Text        string   `json:"text"`
	Origin      string   `json:"origin"`
	Service     []string `json:"service"`
	Value       string   `json:"value"`
}
