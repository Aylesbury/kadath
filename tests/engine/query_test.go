package engine_test

import (
	"encoding/json"
	"testing"

	"starless/kadath/internal/types"
)

func TestParseQueryParams(t *testing.T) {
	tests := []struct {
		name        string
		payload     string
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid simple query",
			payload: `{
				"table": "users"
			}`,
			expectError: false,
		},
		{
			name: "valid query with all fields",
			payload: `{
				"database_type": "postgres",
				"schema_name": "public",
				"table": "users",
				"select": "id, name, email",
				"conditions": [
					{"column": "status", "type": "equal", "value": "active"}
				],
				"limit": 100,
				"group_by": ["status"],
				"having": [
					{"column": "count", "type": "greater_than", "value": 5}
				]
			}`,
			expectError: false,
		},
		{
			name: "missing table",
			payload: `{
				"select": "id, name"
			}`,
			expectError: true,
			errorMsg:    "table is required",
		},
		{
			name: "condition without column",
			payload: `{
				"table": "users",
				"conditions": [
					{"type": "equal", "value": "test"}
				]
			}`,
			expectError: true,
			errorMsg:    "column is required",
		},
		{
			name: "condition without type",
			payload: `{
				"table": "users",
				"conditions": [
					{"column": "status", "value": "active"}
				]
			}`,
			expectError: true,
			errorMsg:    "type is required",
		},
		{
			name: "having without column",
			payload: `{
				"table": "users",
				"having": [
					{"type": "greater_than", "value": 5}
				]
			}`,
			expectError: true,
			errorMsg:    "column is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params, err := types.ParseQueryParams(tt.payload)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error containing '%s', got nil", tt.errorMsg)
				} else if tt.errorMsg != "" && !contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if params == nil {
					t.Error("expected params, got nil")
				}
			}
		})
	}
}

func TestQueryParamsValidate(t *testing.T) {
	tests := []struct {
		name        string
		params      types.QueryParams
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid minimal params",
			params: types.QueryParams{
				Table: "users",
			},
			expectError: false,
		},
		{
			name: "empty table",
			params: types.QueryParams{
				Table: "",
			},
			expectError: true,
			errorMsg:    "table is required",
		},
		{
			name: "valid with conditions",
			params: types.QueryParams{
				Table: "users",
				Conditions: []types.Condition{
					{Column: "id", Type: types.ConditionTypeEqual, Value: 1},
				},
			},
			expectError: false,
		},
		{
			name: "condition missing column",
			params: types.QueryParams{
				Table: "users",
				Conditions: []types.Condition{
					{Type: types.ConditionTypeEqual, Value: 1},
				},
			},
			expectError: true,
			errorMsg:    "column is required",
		},
		{
			name: "condition missing type",
			params: types.QueryParams{
				Table: "users",
				Conditions: []types.Condition{
					{Column: "id", Value: 1},
				},
			},
			expectError: true,
			errorMsg:    "type is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.params.Validate()

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error containing '%s', got nil", tt.errorMsg)
				} else if !contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestQueryResponse(t *testing.T) {
	response := types.QueryResponse{
		Rows: []types.QueryResult{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		},
		RowCount: 2,
	}

	// Test JSON serialization
	jsonData, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("failed to marshal response: %v", err)
	}

	// Test JSON deserialization
	var decoded types.QueryResponse
	if err := json.Unmarshal(jsonData, &decoded); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if decoded.RowCount != 2 {
		t.Errorf("expected row_count=2, got %d", decoded.RowCount)
	}

	if len(decoded.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(decoded.Rows))
	}
}

func TestConditionTypes(t *testing.T) {
	validTypes := []types.ConditionType{
		types.ConditionTypeEqual,
		types.ConditionTypeNotEqual,
		types.ConditionTypeGreaterThan,
		types.ConditionTypeGreaterThanOrEqual,
		types.ConditionTypeLessThan,
		types.ConditionTypeLessThanOrEqual,
		types.ConditionTypeLike,
		types.ConditionTypeIn,
		types.ConditionTypeIsNull,
		types.ConditionTypeIsNotNull,
	}

	for _, ct := range validTypes {
		if string(ct) == "" {
			t.Errorf("condition type should not be empty")
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && stringContains(s, substr)))
}

func stringContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
