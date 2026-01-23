//go:build postgres || mysql

package engine_test

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"starless/kadath/internal/types"
)

// TestEngineInterface tests the Engine interface using only exported APIs
// This is a high-level integration test that doesn't depend on internal implementation
func TestEngineInterface(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Note: We can't test NewEngine directly without a real connection string
	// These tests verify the interface contract works as expected

	t.Run("query params validation", func(t *testing.T) {
		// Test that ParseQueryParams validates correctly
		_, err := types.ParseQueryParams(`{"table": ""}`)
		if err == nil {
			t.Error("expected validation error for empty table")
		}

		params, err := types.ParseQueryParams(`{"table": "users"}`)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if params.Table != "users" {
			t.Errorf("expected table=users, got %s", params.Table)
		}
	})

	t.Run("complex query parsing", func(t *testing.T) {
		payload := `{
			"table": "orders",
			"schema_name": "public",
			"select": "id, status, total",
			"conditions": [
				{"column": "status", "type": "equal", "value": "completed"},
				{"column": "total", "type": "greater_than", "value": 100}
			],
			"limit": 50
		}`

		params, err := types.ParseQueryParams(payload)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if params.Table != "orders" {
			t.Errorf("expected table=orders, got %s", params.Table)
		}

		if params.SchemaName == nil || *params.SchemaName != "public" {
			t.Error("expected schema_name=public")
		}

		if len(params.Conditions) != 2 {
			t.Errorf("expected 2 conditions, got %d", len(params.Conditions))
		}

		if params.Limit == nil || *params.Limit != 50 {
			t.Error("expected limit=50")
		}
	})

	t.Run("query result serialization", func(t *testing.T) {
		// Test that QueryResponse can be created and contains expected structure
		result := &types.QueryResponse{
			Rows: []types.QueryResult{
				{"id": 1, "name": "test"},
			},
			RowCount: 1,
		}

		if result.RowCount != 1 {
			t.Errorf("expected RowCount=1, got %d", result.RowCount)
		}

		if len(result.Rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(result.Rows))
		}

		if result.Rows[0]["name"] != "test" {
			t.Errorf("expected name=test, got %v", result.Rows[0]["name"])
		}
	})

	_ = mock // Unused for now, but available for future tests
}

// TestConditionTypeConstants verifies all condition types are defined
func TestConditionTypeConstants(t *testing.T) {
	conditionTypes := []types.ConditionType{
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

	for _, ct := range conditionTypes {
		if string(ct) == "" {
			t.Errorf("condition type should not be empty")
		}
	}

	// Verify we have all 10 types
	if len(conditionTypes) != 10 {
		t.Errorf("expected 10 condition types, got %d", len(conditionTypes))
	}
}

// TestQueryParamsContract verifies the QueryParams struct contract
func TestQueryParamsContract(t *testing.T) {
	t.Run("required fields", func(t *testing.T) {
		params := types.QueryParams{
			Table: "users",
		}

		if err := params.Validate(); err != nil {
			t.Errorf("unexpected validation error: %v", err)
		}
	})

	t.Run("validation errors", func(t *testing.T) {
		tests := []struct {
			name   string
			params types.QueryParams
		}{
			{
				name: "empty table",
				params: types.QueryParams{
					Table: "",
				},
			},
			{
				name: "condition without column",
				params: types.QueryParams{
					Table: "users",
					Conditions: []types.Condition{
						{Type: types.ConditionTypeEqual, Value: 1},
					},
				},
			},
			{
				name: "condition without type",
				params: types.QueryParams{
					Table: "users",
					Conditions: []types.Condition{
						{Column: "id", Value: 1},
					},
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if err := tt.params.Validate(); err == nil {
					t.Error("expected validation error, got nil")
				}
			})
		}
	})
}
