package queries

import (
	"context"
	"net/http"

	"github.com/shomali11/xsql"
	"github.com/sqlpipe/sqlpipe/internal/data"
)

func RunQuery(ctx context.Context, query data.Query) (string, int, error) {
	rows, err := query.Source.Db.QueryContext(ctx, query.Query)
	if err != nil {
		switch {
		case err.Error() == `Stmt did not create a result set`:
			return "Query ran successfully, but did not product a result set.", http.StatusOK, nil
		default:
			return "", http.StatusBadRequest, err
		}
	}

	prettyRows, err := xsql.Pretty(rows)
	if err != nil {
		return "", http.StatusInternalServerError, err
	}

	return prettyRows, http.StatusOK, nil
}
