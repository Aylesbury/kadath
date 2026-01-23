//go:build postgres

package postgres

import (
	"reflect"
	"testing"

	"starless/kadath/configs"
	"starless/kadath/internal/types"
)

func TestPostgresBuildQuery(t *testing.T) {
	eng := &postgresEngine{}

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
				SchemaName: stringPtr("public"),
			},
			expectedQuery: "SELECT * FROM public.users",
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
			expectedQuery: "SELECT * FROM users WHERE status = $1",
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
			expectedQuery: "SELECT * FROM users WHERE status = $1 AND age > $2",
			expectedArgs:  []interface{}{"active", 18},
			expectError:   false,
		},
		{
			name: "with limit",
			params: &types.QueryParams{
				Table: "users",
				Limit: intPtr(10),
			},
			expectedQuery: "SELECT * FROM users LIMIT $1",
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
			expectedQuery: "SELECT status, COUNT(*) as count FROM orders GROUP BY status HAVING COUNT(*) > $1",
			expectedArgs:  []interface{}{5},
			expectError:   false,
		},
		{
			name: "complex query",
			params: &types.QueryParams{
				Table:      "users",
				SchemaName: stringPtr("public"),
				Select:     stringPtr("id, name, created_at"),
				Conditions: []types.Condition{
					{Column: "status", Type: types.ConditionTypeEqual, Value: "active"},
					{Column: "email", Type: types.ConditionTypeLike, Value: "%@example.com"},
				},
				Limit: intPtr(50),
			},
			expectedQuery: "SELECT id, name, created_at FROM public.users WHERE status = $1 AND email LIKE $2 LIMIT $3",
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

func TestPostgresBuildCondition(t *testing.T) {
	eng := &postgresEngine{}

	tests := []struct {
		name           string
		condition      types.Condition
		startIndex     int
		expectedClause string
		expectedArgs   []interface{}
		expectedIndex  int
		expectError    bool
	}{
		{
			name:           "equal condition",
			condition:      types.Condition{Column: "id", Type: types.ConditionTypeEqual, Value: 123},
			startIndex:     1,
			expectedClause: "id = $1",
			expectedArgs:   []interface{}{123},
			expectedIndex:  2,
			expectError:    false,
		},
		{
			name:           "not equal condition",
			condition:      types.Condition{Column: "status", Type: types.ConditionTypeNotEqual, Value: "deleted"},
			startIndex:     1,
			expectedClause: "status != $1",
			expectedArgs:   []interface{}{"deleted"},
			expectedIndex:  2,
			expectError:    false,
		},
		{
			name:           "greater than condition",
			condition:      types.Condition{Column: "age", Type: types.ConditionTypeGreaterThan, Value: 18},
			startIndex:     3,
			expectedClause: "age > $3",
			expectedArgs:   []interface{}{18},
			expectedIndex:  4,
			expectError:    false,
		},
		{
			name:           "less than or equal condition",
			condition:      types.Condition{Column: "price", Type: types.ConditionTypeLessThanOrEqual, Value: 100.50},
			startIndex:     1,
			expectedClause: "price <= $1",
			expectedArgs:   []interface{}{100.50},
			expectedIndex:  2,
			expectError:    false,
		},
		{
			name:           "like condition",
			condition:      types.Condition{Column: "name", Type: types.ConditionTypeLike, Value: "John%"},
			startIndex:     1,
			expectedClause: "name LIKE $1",
			expectedArgs:   []interface{}{"John%"},
			expectedIndex:  2,
			expectError:    false,
		},
		{
			name:           "in condition",
			condition:      types.Condition{Column: "status", Type: types.ConditionTypeIn, Value: []string{"active", "pending"}},
			startIndex:     1,
			expectedClause: "status = ANY($1)",
			expectedArgs:   []interface{}{[]string{"active", "pending"}},
			expectedIndex:  2,
			expectError:    false,
		},
		{
			name:           "is null condition",
			condition:      types.Condition{Column: "deleted_at", Type: types.ConditionTypeIsNull},
			startIndex:     1,
			expectedClause: "deleted_at IS NULL",
			expectedArgs:   []interface{}{},
			expectedIndex:  1,
			expectError:    false,
		},
		{
			name:           "is not null condition",
			condition:      types.Condition{Column: "email", Type: types.ConditionTypeIsNotNull},
			startIndex:     5,
			expectedClause: "email IS NOT NULL",
			expectedArgs:   []interface{}{},
			expectedIndex:  5,
			expectError:    false,
		},
		{
			name:        "unsupported condition type",
			condition:   types.Condition{Column: "foo", Type: types.ConditionType("invalid")},
			startIndex:  1,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause, args, index, err := eng.buildCondition(tt.condition, tt.startIndex)

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

			if index != tt.expectedIndex {
				t.Errorf("index mismatch: expected %d, got %d", tt.expectedIndex, index)
			}
		})
	}
}

func TestPostgresBuildDSN(t *testing.T) {
	tests := []struct {
		name     string
		baseDSN  string
		sslMode  string
		expected string
	}{
		{
			name:     "add sslmode to simple DSN",
			baseDSN:  "postgres://user:pass@localhost/dbname",
			sslMode:  "disable",
			expected: "postgres://user:pass@localhost/dbname?sslmode=disable",
		},
		{
			name:     "add sslmode to DSN with existing params",
			baseDSN:  "postgres://user:pass@localhost/dbname?connect_timeout=10",
			sslMode:  "require",
			expected: "postgres://user:pass@localhost/dbname?connect_timeout=10&sslmode=require",
		},
		{
			name:     "don't modify DSN with existing sslmode",
			baseDSN:  "postgres://user:pass@localhost/dbname?sslmode=verify-full",
			sslMode:  "disable",
			expected: "postgres://user:pass@localhost/dbname?sslmode=verify-full",
		},
		{
			name:     "handle different sslmode values",
			baseDSN:  "postgres://localhost/db",
			sslMode:  "verify-ca",
			expected: "postgres://localhost/db?sslmode=verify-ca",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildDSN(tt.baseDSN, tt.sslMode)
			if result != tt.expected {
				t.Errorf("buildDSN() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestPostgresNewEngine(t *testing.T) {
	// Test that NewEngine doesn't panic with invalid DSN
	// We can't test actual connection without a real postgres instance
	cfg := &configs.Config{
		DSN:     "invalid-dsn",
		SSLMode: "disable",
	}

	_, err := NewEngine(cfg)
	// Should not panic, but may return error depending on driver behavior
	// The driver might not validate the DSN until actual connection
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
