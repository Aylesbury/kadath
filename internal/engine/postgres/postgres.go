//go:build postgres

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
	"starless/kadath/configs"
	"starless/kadath/internal/types"
)

type postgresEngine struct {
	db *sql.DB
}

// buildDSN adds SSL mode to the DSN if not already present
func buildDSN(baseDSN, sslMode string) string {
	// Check if DSN already contains sslmode parameter
	if strings.Contains(baseDSN, "sslmode=") {
		return baseDSN
	}

	// Add sslmode parameter
	separator := "?"
	if strings.Contains(baseDSN, "?") {
		separator = "&"
	}

	return fmt.Sprintf("%s%ssslmode=%s", baseDSN, separator, sslMode)
}

func NewEngine(cfg *configs.Config) (types.Engine, error) {
	dsn := buildDSN(cfg.DSN, cfg.SSLMode)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	return &postgresEngine{
		db: db,
	}, nil
}

func (e *postgresEngine) buildQuery(params *types.QueryParams) (string, []interface{}, error) {
	var query string
	var args []interface{}
	argIndex := 1

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
			clause, condArgs, newIndex, err := e.buildCondition(cond, argIndex)
			if err != nil {
				return "", nil, fmt.Errorf("failed to build condition: %w", err)
			}
			whereClauses = append(whereClauses, clause)
			args = append(args, condArgs...)
			argIndex = newIndex
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
			clause, condArgs, newIndex, err := e.buildCondition(cond, argIndex)
			if err != nil {
				return "", nil, fmt.Errorf("failed to build having condition: %w", err)
			}
			havingClauses = append(havingClauses, clause)
			args = append(args, condArgs...)
			argIndex = newIndex
		}
		query += " HAVING " + types.JoinWithAnd(havingClauses)
	}

	// LIMIT clause
	if params.Limit != nil && *params.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIndex)
		args = append(args, *params.Limit)
	}

	return query, args, nil
}

func (e *postgresEngine) buildCondition(cond types.Condition, startIndex int) (string, []interface{}, int, error) {
	var clause string
	var args []interface{}
	currentIndex := startIndex

	switch cond.Type {
	case types.ConditionTypeEqual:
		clause = fmt.Sprintf("%s = $%d", cond.Column, currentIndex)
		args = append(args, cond.Value)
		currentIndex++

	case types.ConditionTypeNotEqual:
		clause = fmt.Sprintf("%s != $%d", cond.Column, currentIndex)
		args = append(args, cond.Value)
		currentIndex++

	case types.ConditionTypeGreaterThan:
		clause = fmt.Sprintf("%s > $%d", cond.Column, currentIndex)
		args = append(args, cond.Value)
		currentIndex++

	case types.ConditionTypeGreaterThanOrEqual:
		clause = fmt.Sprintf("%s >= $%d", cond.Column, currentIndex)
		args = append(args, cond.Value)
		currentIndex++

	case types.ConditionTypeLessThan:
		clause = fmt.Sprintf("%s < $%d", cond.Column, currentIndex)
		args = append(args, cond.Value)
		currentIndex++

	case types.ConditionTypeLessThanOrEqual:
		clause = fmt.Sprintf("%s <= $%d", cond.Column, currentIndex)
		args = append(args, cond.Value)
		currentIndex++

	case types.ConditionTypeLike:
		clause = fmt.Sprintf("%s LIKE $%d", cond.Column, currentIndex)
		args = append(args, cond.Value)
		currentIndex++

	case types.ConditionTypeIn:
		// For IN clause, value should be a slice
		clause = fmt.Sprintf("%s = ANY($%d)", cond.Column, currentIndex)
		args = append(args, cond.Value)
		currentIndex++

	case types.ConditionTypeIsNull:
		clause = fmt.Sprintf("%s IS NULL", cond.Column)

	case types.ConditionTypeIsNotNull:
		clause = fmt.Sprintf("%s IS NOT NULL", cond.Column)

	default:
		return "", nil, currentIndex, fmt.Errorf("unsupported condition type: %s", cond.Type)
	}

	return clause, args, currentIndex, nil
}

func (e *postgresEngine) Ping(ctx context.Context) error {
	// Use SELECT 1 to check if database is reachable
	var result int
	err := e.db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("postgres ping failed: %w", err)
	}

	if result != 1 {
		return fmt.Errorf("postgres ping returned unexpected result: %d", result)
	}

	return nil
}

func (e *postgresEngine) ExecuteQuery(ctx context.Context, params *types.QueryParams) (*types.QueryResponse, error) {
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

func (e *postgresEngine) Close() error {
	if e.db != nil {
		return e.db.Close()
	}
	return nil
}
