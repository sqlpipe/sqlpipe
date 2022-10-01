package queries

import (
	"context"
	"net/http"

	"github.com/sqlpipe/sqlpipe/internal/data"
)

func RunQuery(ctx context.Context, query data.Query) (map[string]any, int, error) {
	_, err := query.Source.Db.ExecContext(ctx, query.Query)
	if err != nil {
		switch {
		case err.Error() == `Stmt did not create a result set`:
			return map[string]any{"message": "success"}, http.StatusOK, nil
		default:
			return map[string]any{}, http.StatusBadRequest, err
		}
	}

	return map[string]any{"message": "success"}, http.StatusOK, nil
}
