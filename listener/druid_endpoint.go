package listener

import (
	"encoding/json"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"net/http"
	"strings"
	"time"
)

// DruidEmittedData is the data structure which druid sends over HTTP
type DruidEmittedData struct {
	Feed      string    `json:"feed"`
	Timestamp time.Time `json:"timestamp"`
	Service   string    `json:"service"`
	Host      string    `json:"host"`
	Version   string    `json:"version"`
	Metric    string    `json:"metric"`
	Value     int       `json:"value"`
}

// DruidHTTPEndpoint is the endpoint to listen all druid metrics
func DruidHTTPEndpoint(gauge *prometheus.GaugeVec) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var druidData []DruidEmittedData
		if req.Method == "POST" {
			jsonDecoder := json.NewDecoder(req.Body)
			err := jsonDecoder.Decode(&druidData)
			if err != nil {
				log.Error().Msg("Error while decoding JSON sent by druid")
			}
			for _, data := range druidData {
				gauge.With(prometheus.Labels{
					"metric_name": strings.Replace(data.Metric, "/", "-", 3),
					"service":     strings.Replace(data.Service, "/", "-", 3),
					"host":        data.Host}).Set(float64(data.Value))
			}
		}
	})
}
