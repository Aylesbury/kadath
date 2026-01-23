//go:build mysql

package mysql

import (
	"reflect"
	"starless/kadath/internal/types"
	"testing"

	"starless/kadath/configs"
)

func TestMySQLBuildQuery(t *testing.T) {
	eng := &mysqlEngine{}

	tests := []struct {
		name          string
		params        *types.QueryParams
		expectedQuery string
		expectedArgs  []interface{}
		expectError   bool
	}{
		{
			name: "simple select all",
			params: &types.QueryParams{
				Table: "users",
			},
			expectedQuery: "SELECT * FROM users",
			expectedArgs:  []interface{}{},
			expectError:   false,
		},
		{
			name: "select with schema",
			params: &types.QueryParams{
				Table:      "users",
				SchemaName: stringPtr("mydb"),
			},
			expectedQuery: "SELECT * FROM mydb.users",
			expectedArgs:  []interface{}{},
			expectError:   false,
		},
		{
			name: "select specific columns",
			params: &types.QueryParams{
				Table:  "users",
				Select: stringPtr("id, name, email"),
			},
			expectedQuery: "SELECT id, name, email FROM users",
			expectedArgs:  []interface{}{},
			expectError:   false,
		},
		{
			name: "where condition equal",
			params: &types.QueryParams{
				Table: "users",
				Conditions: []types.Condition{
					{Column: "status", Type: types.ConditionTypeEqual, Value: "active"},
				},
			},
			expectedQuery: "SELECT * FROM users WHERE status = ?",
			expectedArgs:  []interface{}{"active"},
			expectError:   false,
		},
		{
			name: "multiple where conditions",
			params: &types.QueryParams{
				Table: "users",
				Conditions: []types.Condition{
					{Column: "status", Type: types.ConditionTypeEqual, Value: "active"},
					{Column: "age", Type: types.ConditionTypeGreaterThan, Value: 18},
				},
			},
			expectedQuery: "SELECT * FROM users WHERE status = ? AND age > ?",
			expectedArgs:  []interface{}{"active", 18},
			expectError:   false,
		},
		{
			name: "with limit",
			params: &types.QueryParams{
				Table: "users",
				Limit: intPtr(10),
			},
			expectedQuery: "SELECT * FROM users LIMIT ?",
			expectedArgs:  []interface{}{10},
			expectError:   false,
		},
		{
			name: "with group by",
			params: &types.QueryParams{
				Table:   "orders",
				Select:  stringPtr("status, COUNT(*) as count"),
				GroupBy: []string{"status"},
			},
			expectedQuery: "SELECT status, COUNT(*) as count FROM orders GROUP BY status",
			expectedArgs:  []interface{}{},
			expectError:   false,
		},
		{
			name: "with having",
			params: &types.QueryParams{
				Table:   "orders",
				Select:  stringPtr("status, COUNT(*) as count"),
				GroupBy: []string{"status"},
				Having: []types.Condition{
					{Column: "COUNT(*)", Type: types.ConditionTypeGreaterThan, Value: 5},
				},
			},
			expectedQuery: "SELECT status, COUNT(*) as count FROM orders GROUP BY status HAVING COUNT(*) > ?",
			expectedArgs:  []interface{}{5},
			expectError:   false,
		},
		{
			name: "complex query",
			params: &types.QueryParams{
				Table:      "users",
				SchemaName: stringPtr("mydb"),
				Select:     stringPtr("id, name, created_at"),
				Conditions: []types.Condition{
					{Column: "status", Type: types.ConditionTypeEqual, Value: "active"},
					{Column: "email", Type: types.ConditionTypeLike, Value: "%@example.com"},
				},
				Limit: intPtr(50),
			},
			expectedQuery: "SELECT id, name, created_at FROM mydb.users WHERE status = ? AND email LIKE ? LIMIT ?",
			expectedArgs:  []interface{}{"active", "%@example.com", 50},
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args, err := eng.buildQuery(tt.params)

			if tt.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if query != tt.expectedQuery {
				t.Errorf("query mismatch:\nexpected: %s\ngot:      %s", tt.expectedQuery, query)
			}

			if !argsEqual(args, tt.expectedArgs) {
				t.Errorf("args mismatch:\nexpected: %v\ngot:      %v", tt.expectedArgs, args)
			}
		})
	}
}

func TestMySQLBuildCondition(t *testing.T) {
	eng := &mysqlEngine{}

	tests := []struct {
		name           string
		condition      types.Condition
		expectedClause string
		expectedArgs   []interface{}
		expectError    bool
	}{
		{
			name:           "equal condition",
			condition:      types.Condition{Column: "id", Type: types.ConditionTypeEqual, Value: 123},
			expectedClause: "id = ?",
			expectedArgs:   []interface{}{123},
			expectError:    false,
		},
		{
			name:           "not equal condition",
			condition:      types.Condition{Column: "status", Type: types.ConditionTypeNotEqual, Value: "deleted"},
			expectedClause: "status != ?",
			expectedArgs:   []interface{}{"deleted"},
			expectError:    false,
		},
		{
			name:           "greater than condition",
			condition:      types.Condition{Column: "age", Type: types.ConditionTypeGreaterThan, Value: 18},
			expectedClause: "age > ?",
			expectedArgs:   []interface{}{18},
			expectError:    false,
		},
		{
			name:           "greater than or equal condition",
			condition:      types.Condition{Column: "score", Type: types.ConditionTypeGreaterThanOrEqual, Value: 90},
			expectedClause: "score >= ?",
			expectedArgs:   []interface{}{90},
			expectError:    false,
		},
		{
			name:           "less than condition",
			condition:      types.Condition{Column: "price", Type: types.ConditionTypeLessThan, Value: 99.99},
			expectedClause: "price < ?",
			expectedArgs:   []interface{}{99.99},
			expectError:    false,
		},
		{
			name:           "less than or equal condition",
			condition:      types.Condition{Column: "price", Type: types.ConditionTypeLessThanOrEqual, Value: 100.50},
			expectedClause: "price <= ?",
			expectedArgs:   []interface{}{100.50},
			expectError:    false,
		},
		{
			name:           "like condition",
			condition:      types.Condition{Column: "name", Type: types.ConditionTypeLike, Value: "John%"},
			expectedClause: "name LIKE ?",
			expectedArgs:   []interface{}{"John%"},
			expectError:    false,
		},
		{
			name:           "in condition",
			condition:      types.Condition{Column: "status", Type: types.ConditionTypeIn, Value: []string{"active", "pending"}},
			expectedClause: "status IN (?)",
			expectedArgs:   []interface{}{[]string{"active", "pending"}},
			expectError:    false,
		},
		{
			name:           "is null condition",
			condition:      types.Condition{Column: "deleted_at", Type: types.ConditionTypeIsNull},
			expectedClause: "deleted_at IS NULL",
			expectedArgs:   []interface{}{},
			expectError:    false,
		},
		{
			name:           "is not null condition",
			condition:      types.Condition{Column: "email", Type: types.ConditionTypeIsNotNull},
			expectedClause: "email IS NOT NULL",
			expectedArgs:   []interface{}{},
			expectError:    false,
		},
		{
			name:        "unsupported condition type",
			condition:   types.Condition{Column: "foo", Type: types.ConditionType("invalid")},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause, args, err := eng.buildCondition(tt.condition)

			if tt.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if clause != tt.expectedClause {
				t.Errorf("clause mismatch:\nexpected: %s\ngot:      %s", tt.expectedClause, clause)
			}

			if !argsEqual(args, tt.expectedArgs) {
				t.Errorf("args mismatch:\nexpected: %v\ngot:      %v", tt.expectedArgs, args)
			}
		})
	}
}

func TestMySQLNewEngine(t *testing.T) {
	// Test that newEngine doesn't panic with invalid DSN
	// We can't test actual connection without a real mysql instance
	cfg := &configs.Config{
		DSN: "invalid-dsn",
	}

	_, err := NewEngine(cfg)
	// Should not panic, but may return error depending on driver behavior
	_ = err
}

func stringPtr(s string) *string {
	return &s
}

func intPtr(i int) *int {
	return &i
}

func argsEqual(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}
