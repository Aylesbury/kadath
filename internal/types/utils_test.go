package types

import "testing"

func TestJoinWithAnd(t *testing.T) {
	tests := []struct {
		name     string
		clauses  []string
		expected string
	}{
		{
			name:     "empty clauses",
			clauses:  []string{},
			expected: "",
		},
		{
			name:     "single clause",
			clauses:  []string{"id = 1"},
			expected: "id = 1",
		},
		{
			name:     "two clauses",
			clauses:  []string{"id = 1", "status = 'active'"},
			expected: "id = 1 AND status = 'active'",
		},
		{
			name:     "multiple clauses",
			clauses:  []string{"id > 10", "status = 'active'", "created_at > '2024-01-01'"},
			expected: "id > 10 AND status = 'active' AND created_at > '2024-01-01'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := JoinWithAnd(tt.clauses)
			if result != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestJoinColumns(t *testing.T) {
	tests := []struct {
		name     string
		columns  []string
		expected string
	}{
		{
			name:     "empty columns",
			columns:  []string{},
			expected: "",
		},
		{
			name:     "single column",
			columns:  []string{"id"},
			expected: "id",
		},
		{
			name:     "two columns",
			columns:  []string{"id", "name"},
			expected: "id, name",
		},
		{
			name:     "multiple columns",
			columns:  []string{"id", "name", "email", "created_at"},
			expected: "id, name, email, created_at",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := JoinColumns(tt.columns)
			if result != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}
