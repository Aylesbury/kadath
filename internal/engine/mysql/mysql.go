//go:build mysql

package mysql

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"

	"starless/kadath/configs"
	"starless/kadath/internal/types"
)

type mysqlEngine struct {
	db *sql.DB
}

func NewEngine(cfg *configs.Config) (types.Engine, error) {
	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql connection: %w", err)
	}

	return &mysqlEngine{
		db: db,
	}, nil
}

func (e *mysqlEngine) buildQuery(params *types.QueryParams) (string, []interface{}, error) {
	var query string
	var args []interface{}

	// SELECT clause
	selectClause := "*"
	if params.Select != nil && *params.Select != "" {
		selectClause = *params.Select
	}

	// FROM clause with optional schema
	tableName := params.Table
	if params.SchemaName != nil && *params.SchemaName != "" {
		tableName = fmt.Sprintf("%s.%s", *params.SchemaName, params.Table)
	}

	query = fmt.Sprintf("SELECT %s FROM %s", selectClause, tableName)

	// WHERE clause
	if len(params.Conditions) > 0 {
		whereClauses := []string{}
		for _, cond := range params.Conditions {
			clause, condArgs, err := e.buildCondition(cond)
			if err != nil {
				return "", nil, fmt.Errorf("failed to build condition: %w", err)
			}
			whereClauses = append(whereClauses, clause)
			args = append(args, condArgs...)
		}
		query += " WHERE " + types.JoinWithAnd(whereClauses)
	}

	// GROUP BY clause
	if len(params.GroupBy) > 0 {
		query += " GROUP BY " + types.JoinColumns(params.GroupBy)
	}

	// HAVING clause
	if len(params.Having) > 0 {
		havingClauses := []string{}
		for _, cond := range params.Having {
			clause, condArgs, err := e.buildCondition(cond)
			if err != nil {
				return "", nil, fmt.Errorf("failed to build having condition: %w", err)
			}
			havingClauses = append(havingClauses, clause)
			args = append(args, condArgs...)
		}
		query += " HAVING " + types.JoinWithAnd(havingClauses)
	}

	// LIMIT clause
	if params.Limit != nil && *params.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, *params.Limit)
	}

	return query, args, nil
}

func (e *mysqlEngine) buildCondition(cond types.Condition) (string, []interface{}, error) {
	var clause string
	var args []interface{}

	switch cond.Type {
	case types.ConditionTypeEqual:
		clause = fmt.Sprintf("%s = ?", cond.Column)
		args = append(args, cond.Value)

	case types.ConditionTypeNotEqual:
		clause = fmt.Sprintf("%s != ?", cond.Column)
		args = append(args, cond.Value)

	case types.ConditionTypeGreaterThan:
		clause = fmt.Sprintf("%s > ?", cond.Column)
		args = append(args, cond.Value)

	case types.ConditionTypeGreaterThanOrEqual:
		clause = fmt.Sprintf("%s >= ?", cond.Column)
		args = append(args, cond.Value)

	case types.ConditionTypeLessThan:
		clause = fmt.Sprintf("%s < ?", cond.Column)
		args = append(args, cond.Value)

	case types.ConditionTypeLessThanOrEqual:
		clause = fmt.Sprintf("%s <= ?", cond.Column)
		args = append(args, cond.Value)

	case types.ConditionTypeLike:
		clause = fmt.Sprintf("%s LIKE ?", cond.Column)
		args = append(args, cond.Value)

	case types.ConditionTypeIn:
		// For MySQL IN clause with a slice
		// Note: This is a simplified version. For proper IN support,
		// we'd need to expand the slice into multiple ? placeholders
		clause = fmt.Sprintf("%s IN (?)", cond.Column)
		args = append(args, cond.Value)

	case types.ConditionTypeIsNull:
		clause = fmt.Sprintf("%s IS NULL", cond.Column)

	case types.ConditionTypeIsNotNull:
		clause = fmt.Sprintf("%s IS NOT NULL", cond.Column)

	default:
		return "", nil, fmt.Errorf("unsupported condition type: %s", cond.Type)
	}

	return clause, args, nil
}

func (e *mysqlEngine) Ping(ctx context.Context) error {
	// Use SELECT 1 to check if database is reachable
	var result int
	err := e.db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("mysql ping failed: %w", err)
	}

	if result != 1 {
		return fmt.Errorf("mysql ping returned unexpected result: %d", result)
	}

	return nil
}

func (e *mysqlEngine) ExecuteQuery(ctx context.Context, params *types.QueryParams) (*types.QueryResponse, error) {
	query, args, err := e.buildQuery(params)
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	results := []types.QueryResult{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		row := make(types.QueryResult)
		for i, col := range columns {
			val := values[i]
			// Convert byte arrays to strings for better JSON serialization
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return &types.QueryResponse{
		Rows:     results,
		RowCount: len(results),
	}, nil
}

func (e *mysqlEngine) Close() error {
	if e.db != nil {
		return e.db.Close()
	}
	return nil
}
