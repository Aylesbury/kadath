//go:build postgres

package postgres

import (
	"context"
	"starless/kadath/internal/types"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresExecuteQuery(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	eng := &postgresEngine{db: db}
	ctx := context.Background()

	tests := []struct {
		name        string
		params      *types.QueryParams
		setupMock   func(sqlmock.Sqlmock)
		expectError bool
		validateResult func(*testing.T, *types.QueryResponse)
	}{
		{
			name: "simple select query",
			params: &types.QueryParams{
				Table: "users",
			},
			setupMock: func(m sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "name", "email"}).
					AddRow(1, "Alice", "alice@example.com").
					AddRow(2, "Bob", "bob@example.com")
				m.ExpectQuery("SELECT \\* FROM users").WillReturnRows(rows)
			},
			expectError: false,
			validateResult: func(t *testing.T, result *types.QueryResponse) {
				if result.RowCount != 2 {
					t.Errorf("expected 2 rows, got %d", result.RowCount)
				}
				if len(result.Rows) != 2 {
					t.Errorf("expected 2 rows, got %d", len(result.Rows))
				}
				if result.Rows[0]["name"] != "Alice" {
					t.Errorf("expected Alice, got %v", result.Rows[0]["name"])
				}
			},
		},
		{
			name: "query with where condition",
			params: &types.QueryParams{
				Table: "users",
				Conditions: []types.Condition{
					{Column: "status", Type: types.ConditionTypeEqual, Value: "active"},
				},
			},
			setupMock: func(m sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "name"}).
					AddRow(1, "Alice")
				m.ExpectQuery("SELECT \\* FROM users WHERE status = \\$1").
					WithArgs("active").
					WillReturnRows(rows)
			},
			expectError: false,
			validateResult: func(t *testing.T, result *types.QueryResponse) {
				if result.RowCount != 1 {
					t.Errorf("expected 1 row, got %d", result.RowCount)
				}
			},
		},
		{
			name: "query with limit",
			params: &types.QueryParams{
				Table: "users",
				Limit: intPtr(10),
			},
			setupMock: func(m sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "name"}).
					AddRow(1, "Alice")
				m.ExpectQuery("SELECT \\* FROM users LIMIT \\$1").
					WithArgs(10).
					WillReturnRows(rows)
			},
			expectError: false,
			validateResult: func(t *testing.T, result *types.QueryResponse) {
				if result.RowCount != 1 {
					t.Errorf("expected 1 row, got %d", result.RowCount)
				}
			},
		},
		{
			name: "query with multiple conditions",
			params: &types.QueryParams{
				Table: "orders",
				Conditions: []types.Condition{
					{Column: "status", Type: types.ConditionTypeEqual, Value: "completed"},
					{Column: "total", Type: types.ConditionTypeGreaterThan, Value: 100},
				},
			},
			setupMock: func(m sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "total"}).
					AddRow(1, 150.50).
					AddRow(2, 200.00)
				m.ExpectQuery("SELECT \\* FROM orders WHERE status = \\$1 AND total > \\$2").
					WithArgs("completed", 100).
					WillReturnRows(rows)
			},
			expectError: false,
			validateResult: func(t *testing.T, result *types.QueryResponse) {
				if result.RowCount != 2 {
					t.Errorf("expected 2 rows, got %d", result.RowCount)
				}
			},
		},
		{
			name: "empty result set",
			params: &types.QueryParams{
				Table: "users",
			},
			setupMock: func(m sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "name"})
				m.ExpectQuery("SELECT \\* FROM users").WillReturnRows(rows)
			},
			expectError: false,
			validateResult: func(t *testing.T, result *types.QueryResponse) {
				if result.RowCount != 0 {
					t.Errorf("expected 0 rows, got %d", result.RowCount)
				}
				if len(result.Rows) != 0 {
					t.Errorf("expected empty rows, got %d", len(result.Rows))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock(mock)

			result, err := eng.ExecuteQuery(ctx, tt.params)

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

			if tt.validateResult != nil {
				tt.validateResult(t, result)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unfulfilled expectations: %v", err)
			}
		})
	}
}

func TestPostgresPing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	eng := &postgresEngine{db: db}
	ctx := context.Background()

	t.Run("successful ping", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"result"}).AddRow(1)
		mock.ExpectQuery("SELECT 1").WillReturnRows(rows)

		err := eng.Ping(ctx)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})

	t.Run("ping with wrong result", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"result"}).AddRow(999)
		mock.ExpectQuery("SELECT 1").WillReturnRows(rows)

		err := eng.Ping(ctx)
		if err == nil {
			t.Error("expected error for wrong result, got nil")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}

func TestPostgresClose(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}

	eng := &postgresEngine{db: db}

	mock.ExpectClose()

	err = eng.Close()
	if err != nil {
		t.Errorf("unexpected error on close: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}

	// Test closing nil db
	eng2 := &postgresEngine{db: nil}
	err = eng2.Close()
	if err != nil {
		t.Errorf("unexpected error on close nil db: %v", err)
	}
}
