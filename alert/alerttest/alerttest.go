package alerttest

import (
	"encoding/json"
	"os"

	"github.com/influxdata/kapacitor/alert"
)

type LogTest struct {
	path string
}

func NewLogTest(p string) *LogTest {
	return &LogTest{
		path: p,
	}
}

func (l *LogTest) Data() ([]alert.AlertData, error) {
	f, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var data []alert.AlertData
	for dec.More() {
		ad := alert.AlertData{}
		err := dec.Decode(&ad)
		if err != nil {
			return nil, err
		}
		data = append(data, ad)
	}
	return data, nil
}

func (l *LogTest) Mode() (os.FileMode, error) {
	stat, err := os.Stat(l.path)
	if err != nil {
		return 0, err
	}
	return stat.Mode(), nil
}
