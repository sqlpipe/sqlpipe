package globals

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/sqlpipe/sqlpipe/internal/data"
)

func SendAnonymizedTransferAnalytics(transfer data.Transfer, server bool) (errProperties map[string]string, err error) {
	if Analytics {
		var input struct {
			SourceType     string `json:"sourceType"`
			TargetType     string `json:"targetType"`
			Status         string `json:"status"`
			Overwrite      bool   `json:"overwrite"`
			CreatedAt      string `json:"createdAt"`
			StoppedAt      string `json:"stoppedAt"`
			Server         bool   `json:"server"`
			SQLpipeVersion string `json:"sqlpipeVersion"`
		}

		input.SourceType = transfer.Source.DsType
		input.TargetType = transfer.Target.DsType
		input.Status = transfer.Status
		input.Overwrite = transfer.Overwrite
		input.CreatedAt = PgDate(transfer.CreatedAt)
		input.StoppedAt = PgDate(transfer.StoppedAt)
		input.Server = server
		input.SQLpipeVersion = GitHash

		body, err := json.Marshal(input)
		if err != nil {
			return map[string]string{"error": err.Error()}, errors.New("unable to send anonymized analytics data")
		}

		client := http.Client{Timeout: 5 * time.Second}
		_, _ = client.Post("https://analytics.sqlpipe.com/transfer", "application/json", bytes.NewBuffer(body))
	}
	return errProperties, err
}

func SendAnonymizedQueryAnalytics(query data.Query, server bool) (errProperties map[string]string, err error) {
	if Analytics {
		var input struct {
			ConnectionType string `json:"ConnectionType"`
			Status         string `json:"status"`
			CreatedAt      string `json:"createdAt"`
			StoppedAt      string `json:"StoppedAt"`
			Server         bool   `json:"server"`
			SQLpipeVersion string `json:"sqlpipeVersion"`
		}

		input.ConnectionType = query.Connection.DsType
		input.Status = query.Status
		input.CreatedAt = PgDate(query.CreatedAt)
		input.CreatedAt = PgDate(query.StoppedAt)
		input.Server = server
		input.SQLpipeVersion = GitHash

		body, err := json.Marshal(input)
		if err != nil {
			return map[string]string{"error": err.Error()}, errors.New("unable to send anonymized analytics data")
		}

		client := http.Client{Timeout: 5 * time.Second}
		_, _ = client.Post("https://analytics.sqlpipe.com/query", "application/json", bytes.NewBuffer(body))
	}
	return errProperties, err
}
