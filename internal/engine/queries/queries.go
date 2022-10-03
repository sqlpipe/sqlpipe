package queries

import (
	"context"
	"database/sql"

	"github.com/sqlpipe/sqlpipe/internal/data"
)

func RunQuery(
	ctx context.Context,
	query data.Query,
) (
	rows *sql.Rows,
	err error,
) {
	return query.Source.Db.QueryContext(ctx, query.Query)
}
