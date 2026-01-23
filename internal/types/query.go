package types

import (
	"encoding/json"
	"fmt"
)

// ConditionType defines the type of condition operator
type ConditionType string

const (
	ConditionTypeEqual              ConditionType = "equal"
	ConditionTypeNotEqual           ConditionType = "not_equal"
	ConditionTypeGreaterThan        ConditionType = "greater_than"
	ConditionTypeGreaterThanOrEqual ConditionType = "greater_than_or_equal"
	ConditionTypeLessThan           ConditionType = "less_than"
	ConditionTypeLessThanOrEqual    ConditionType = "less_than_or_equal"
	ConditionTypeLike               ConditionType = "like"
	ConditionTypeIn                 ConditionType = "in"
	ConditionTypeIsNull             ConditionType = "is_null"
	ConditionTypeIsNotNull          ConditionType = "is_not_null"
)

// Condition represents a WHERE or HAVING clause condition
type Condition struct {
	Column string        `json:"column"`
	Type   ConditionType `json:"type"`
	Value  interface{}   `json:"value,omitempty"`
}

// QueryParams represents the DSL query structure matching the Ruby contract
type QueryParams struct {
	DatabaseType string       `json:"database_type,omitempty"`
	SchemaName   *string      `json:"schema_name,omitempty"`
	Table        string       `json:"table"`
	Select       *string      `json:"select,omitempty"`
	Conditions   []Condition  `json:"conditions,omitempty"`
	Limit        *int         `json:"limit,omitempty"`
	GroupBy      []string     `json:"group_by,omitempty"`
	Having       []Condition  `json:"having,omitempty"`
}

// Validate checks if the query parameters are valid
func (q *QueryParams) Validate() error {
	if q.Table == "" {
		return fmt.Errorf("table is required")
	}

	for i, cond := range q.Conditions {
		if cond.Column == "" {
			return fmt.Errorf("condition[%d]: column is required", i)
		}
		if cond.Type == "" {
			return fmt.Errorf("condition[%d]: type is required", i)
		}
	}

	for i, cond := range q.Having {
		if cond.Column == "" {
			return fmt.Errorf("having[%d]: column is required", i)
		}
		if cond.Type == "" {
			return fmt.Errorf("having[%d]: type is required", i)
		}
	}

	return nil
}

// ParseQueryParams parses JSON payload into QueryParams
func ParseQueryParams(payloadJSON string) (*QueryParams, error) {
	var params QueryParams
	if err := json.Unmarshal([]byte(payloadJSON), &params); err != nil {
		return nil, fmt.Errorf("failed to parse query params: %w", err)
	}

	if err := params.Validate(); err != nil {
		return nil, fmt.Errorf("invalid query params: %w", err)
	}

	return &params, nil
}

// QueryResult represents a single row result
type QueryResult map[string]interface{}

// QueryResponse represents the full query response
type QueryResponse struct {
	Rows    []QueryResult `json:"rows"`
	RowCount int          `json:"row_count"`
}
