// pg_plugin_test.go
package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// Mock for DB
type mockDB struct {
	maxOpenConns    int
	maxIdleConns    int
	connMaxLifetime time.Duration
}

func (m *mockDB) SetMaxOpenConns(n int) {
	m.maxOpenConns = n
}

func (m *mockDB) SetMaxIdleConns(n int) {
	m.maxIdleConns = n
}

func (m *mockDB) SetConnMaxLifetime(d time.Duration) {
	m.connMaxLifetime = d
}

func (m *mockDB) Ping() error {
	return nil
}

// Mock for opener
type mockSQLOpener struct {
	mockDB *sql.DB
}

func (o *mockSQLOpener) Open(driverName, dataSourceName string) (*sql.DB, error) {
	return o.mockDB, nil
}

func newTestPlugin(t *testing.T) (*pgPlugin, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("failed to open sqlmock database: %s", err)
	}
	plugin := &pgPlugin{db: db}
	return plugin, mock
}

func TestInitConnectionValid(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectPing()

	plugin := &pgPlugin{
		opener: &mockSQLOpener{mockDB: db},
	}

	err = plugin.InitConnection("postgres://user:pass@localhost:5432/db")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestConvertPlaceholders(t *testing.T) {
	query := "SELECT * FROM users WHERE id = ? AND status = ?"
	converted := convertPlaceholders(query)
	expected := "SELECT * FROM users WHERE id = $1 AND status = $2"
	if converted != expected {
		t.Errorf("expected %s, got %s", expected, converted)
	}
}

func TestTableGet(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()
	selectFields := []string{"id", "name"}
	where := map[string]any{
		"status": map[string]any{"=": "active"},
	}
	mock.ExpectBegin()
	queryRegex := regexp.QuoteMeta(`SELECT id, name FROM users WHERE status = $1`)
	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John Doe")
	mock.ExpectQuery(queryRegex).WithArgs("active").WillReturnRows(rows)
	mock.ExpectCommit()
	result, err := plugin.TableGet("user1", "users", selectFields, where, nil, nil, 0, 0, nil)
	if err != nil {
		t.Fatalf("TableGet error: %s", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 row, got %d", len(result))
	}
	if result[0]["name"] != "John Doe" {
		t.Errorf("expected 'John Doe', got %v", result[0]["name"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestTableGet: %s", err)
	}
}

func TestTableGetSelectFieldSubstitution(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()
	ctx := map[string]any{
		"claims_sub": "sub_value",
	}
	selectFields := []string{"id", "erctx.claims_sub"}
	where := map[string]any{}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("SELECT set_config($1, $2, true)")).
		WithArgs("request.claims_sub", "sub_value").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("SELECT set_config($1, $2, true)")).
		WithArgs("erctx.claims_sub", "sub_value").
		WillReturnResult(sqlmock.NewResult(1, 1))
	expectedQueryRegex := regexp.QuoteMeta("SELECT id, erctx.claims_sub FROM users")
	rows := sqlmock.NewRows([]string{"id", "erctx.claims_sub"}).AddRow(10, "sub_value")
	mock.ExpectQuery(expectedQueryRegex).WithArgs().WillReturnRows(rows)
	mock.ExpectCommit()
	result, err := plugin.TableGet("user1", "users", selectFields, where, nil, nil, 0, 0, ctx)
	if err != nil {
		t.Fatalf("TableGet error: %s", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 row, got %d", len(result))
	}
	if result[0]["erctx.claims_sub"] != "sub_value" {
		t.Errorf("expected 'sub_value', got %v", result[0]["erctx.claims_sub"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestTableGetSelectFieldSubstitution: %s", err)
	}
}

func TestTableGetTimeWithTimezone(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()
	utcTime := time.Date(2025, 3, 7, 15, 30, 0, 0, time.UTC)
	// In America/New_York (UTC-5), expected time becomes 10:30.
	expectedTime := "2025-03-07 10:30:00"
	selectFields := []string{"id", "created_at"}
	where := map[string]any{"dummy": "val"}
	expQ := "SELECT id, created_at FROM dummy WHERE dummy = $1"
	rows := sqlmock.NewRows([]string{"id", "created_at"}).AddRow(1, utcTime)
	mock.ExpectBegin()
	// Expect setting timezone.
	mock.ExpectExec(regexp.QuoteMeta("SELECT set_config($1, $2, true)")).
		WithArgs("request.timezone", "America/New_York").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("SELECT set_config($1, $2, true)")).
		WithArgs("erctx.timezone", "America/New_York").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(expQ)).WithArgs("val").WillReturnRows(rows)
	mock.ExpectCommit()
	ctxData := map[string]any{
		"timezone": "America/New_York",
	}
	results, err := plugin.TableGet("u", "dummy", selectFields, where, nil, nil, 0, 0, ctxData)
	if err != nil {
		t.Fatalf("TableGet error: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 row, got %d", len(results))
	}
	if results[0]["created_at"] != expectedTime {
		t.Errorf("expected time %s, got %v", expectedTime, results[0]["created_at"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestTableGetTimeWithTimezone: %v", err)
	}
}

func TestTableCreate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	defer db.Close()

	plugin := &pgPlugin{
		db:            db,
		bulkThreshold: 1000, // Set high threshold to avoid bulk operations in this test
	}

	data := []map[string]any{
		{"id": 1, "name": "Test1"},
		{"id": 2, "name": "Test2"},
		{"id": 3, "name": "Test3"},
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO users \\(id, name\\) VALUES \\(\\$1,\\$2\\),\\(\\$3,\\$4\\),\\(\\$5,\\$6\\)").
		WithArgs(1, "Test1", 2, "Test2", 3, "Test3").
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectCommit()

	result, err := plugin.TableCreate("test_user", "users", data, nil)
	if err != nil {
		t.Errorf("TableCreate error: %v", err)
	}

	if len(result) != len(data) {
		t.Errorf("Expected %d results, got %d", len(data), len(result))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestTableUpdate(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()
	data := map[string]any{
		"name": "Bob",
	}
	where := map[string]any{
		"id": map[string]any{"=": 1},
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE users SET name = $1 WHERE id = $2`)).
		WithArgs("Bob", 1).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	affected, err := plugin.TableUpdate("user1", "users", data, where, nil)
	if err != nil {
		t.Fatalf("TableUpdate error: %s", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 affected row, got %d", affected)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestTableUpdate: %s", err)
	}
}

func TestTableDelete(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()
	where := map[string]any{
		"status": map[string]any{"=": "inactive"},
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM users WHERE status = $1`)).
		WithArgs("inactive").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	affected, err := plugin.TableDelete("user1", "users", where, nil)
	if err != nil {
		t.Fatalf("TableDelete error: %s", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 affected row, got %d", affected)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestTableDelete: %s", err)
	}
}

func TestCallFunction(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()
	data := map[string]any{
		"param": "value",
	}
	funcName := "doSomething"
	expectedResult := "Processed: value"

	mock.ExpectBegin()
	// Updated query expectation to match the new format SELECT * FROM funcName(...)
	queryRegex := regexp.QuoteMeta(fmt.Sprintf(`SELECT * FROM %s($1)`, funcName))
	// Mock the result based on what processRows expects
	rows := sqlmock.NewRows([]string{"doSomething"}).AddRow(expectedResult)
	mock.ExpectQuery(queryRegex).WithArgs("value").WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := plugin.CallFunction("user1", funcName, data, nil)
	if err != nil {
		t.Fatalf("CallFunction error: %s", err)
	}

	// The modified CallFunction now returns the scalar value directly
	resultStr, ok := result.(string)
	if !ok {
		t.Fatalf("Expected result to be a string, got %T", result)
	}

	if resultStr != expectedResult {
		t.Errorf("expected '%s', got '%v'", expectedResult, resultStr)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestCallFunction: %s", err)
	}
}

func TestGetSchema(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()
	ctxData := map[string]any{
		"timezone": "UTC",
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("SELECT set_config($1, $2, true)")).
		WithArgs("request.timezone", "UTC").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("SELECT set_config($1, $2, true)")).
		WithArgs("erctx.timezone", "UTC").
		WillReturnResult(sqlmock.NewResult(1, 1))
	// Base tables.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")).
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("users").AddRow("orders"))
	// Columns for "users".
	mock.ExpectQuery(regexp.QuoteMeta("SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1")).
		WithArgs("users").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
			AddRow("id", "integer", "NO", nil).
			AddRow("name", "character varying", "YES", nil))
	// Columns for "orders".
	mock.ExpectQuery(regexp.QuoteMeta("SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1")).
		WithArgs("orders").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
			AddRow("id", "integer", "NO", nil).
			AddRow("amount", "numeric", "YES", nil))
	// Views.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'VIEW'")).
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("myview"))
	// Columns for view "myview".
	mock.ExpectQuery(regexp.QuoteMeta("SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1")).
		WithArgs("myview").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
			AddRow("vcol", "text", "YES", nil))
	// RPC functions.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT routine_name, specific_name, data_type FROM information_schema.routines WHERE specific_schema = 'public' AND routine_type = 'FUNCTION'")).
		WillReturnRows(sqlmock.NewRows([]string{"routine_name", "specific_name", "data_type"}).
			AddRow("fnTest", "fnTest", "integer"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT parameter_name, data_type, parameter_mode, ordinal_position FROM information_schema.parameters WHERE specific_schema = 'public' AND specific_name = $1 ORDER BY ordinal_position")).
		WithArgs("fnTest").
		WillReturnRows(sqlmock.NewRows([]string{"parameter_name", "data_type", "parameter_mode", "ordinal_position"}).
			AddRow("x", "integer", "IN", 1))
	mock.ExpectCommit()
	schema, err := plugin.GetSchema(ctxData)
	if err != nil {
		t.Fatalf("GetSchema error: %v", err)
	}
	js, _ := json.MarshalIndent(schema, "", "  ")
	if !strings.Contains(string(js), `"tables"`) {
		t.Errorf("expected 'tables' key, got: %s", js)
	}
	if !strings.Contains(string(js), `"views"`) {
		t.Errorf("expected 'views' key, got: %s", js)
	}
	if !strings.Contains(string(js), `"rpc"`) {
		t.Errorf("expected 'rpc' key, got: %s", js)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestGetSchema: %v", err)
	}
}

func TestInitConnectionWithParams(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectPing()

	plugin := &pgPlugin{
		opener: &mockSQLOpener{mockDB: db},
	}

	uri := "postgres://user:pass@localhost:5432/db?maxOpenConns=50&maxIdleConns=10&connMaxLifetime=15&connMaxIdleTime=20&timeout=60"
	err = plugin.InitConnection(uri)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if plugin.defaultTimeout != 60*time.Second {
		t.Errorf("Expected timeout 60s, got %v", plugin.defaultTimeout)
	}
}

func TestInitConnectionDefaults(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectPing()

	plugin := &pgPlugin{
		opener: &mockSQLOpener{mockDB: db},
	}

	uri := "postgres://user:pass@localhost:5432/db"
	err = plugin.InitConnection(uri)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if plugin.defaultTimeout != 30*time.Second {
		t.Errorf("Expected default timeout 30s, got %v", plugin.defaultTimeout)
	}
}

func TestInitConnectionInvalidURI(t *testing.T) {
	plugin := &pgPlugin{
		opener: &mockSQLOpener{},
	}

	err := plugin.InitConnection("invalid://uri")
	if err == nil {
		t.Error("Expected error for invalid URI")
	}
}

func TestInitConnectionInvalidParams(t *testing.T) {
	tests := []struct {
		name string
		uri  string
	}{
		{
			name: "Invalid maxOpenConns",
			uri:  "postgres://user:pass@localhost:5432/db?maxOpenConns=invalid",
		},
		{
			name: "Invalid maxIdleConns",
			uri:  "postgres://user:pass@localhost:5432/db?maxIdleConns=invalid",
		},
		{
			name: "Invalid connMaxLifetime",
			uri:  "postgres://user:pass@localhost:5432/db?connMaxLifetime=invalid",
		},
		{
			name: "Invalid connMaxIdleTime",
			uri:  "postgres://user:pass@localhost:5432/db?connMaxIdleTime=invalid",
		},
		{
			name: "Invalid timeout",
			uri:  "postgres://user:pass@localhost:5432/db?timeout=invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := &pgPlugin{
				opener: &mockSQLOpener{},
			}

			err := plugin.InitConnection(tt.uri)
			if err == nil {
				t.Error("Expected error for invalid parameter")
			}
		})
	}
}

func TestInitConnectionWithBulkThreshold(t *testing.T) {
	tests := []struct {
		name          string
		uri           string
		expectedValue int
		expectError   bool
	}{
		{
			name:          "Default bulk threshold",
			uri:           "postgres://user:pass@localhost:5432/db",
			expectedValue: 100,
			expectError:   false,
		},
		{
			name:          "Custom bulk threshold",
			uri:           "postgres://user:pass@localhost:5432/db?bulkThreshold=500",
			expectedValue: 500,
			expectError:   false,
		},
		{
			name:          "Invalid bulk threshold",
			uri:           "postgres://user:pass@localhost:5432/db?bulkThreshold=invalid",
			expectedValue: 0,
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("Failed to create mock: %v", err)
			}
			defer db.Close()

			mock.ExpectPing()

			plugin := &pgPlugin{
				opener: &mockSQLOpener{mockDB: db},
			}

			err = plugin.InitConnection(tt.uri)
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if plugin.bulkThreshold != tt.expectedValue {
				t.Errorf("Expected bulk threshold %d, got %d", tt.expectedValue, plugin.bulkThreshold)
			}
		})
	}
}

func TestTableCreateWithBulkOperations(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	defer db.Close()

	plugin := &pgPlugin{
		db:            db,
		bulkThreshold: 2, // Set low threshold for testing
	}

	tests := []struct {
		name        string
		data        []map[string]any
		expectBulk  bool
		expectError bool
	}{
		{
			name: "Regular insert",
			data: []map[string]any{
				{"id": 1, "name": "Test1"},
			},
			expectBulk:  false,
			expectError: false,
		},
		{
			name: "Bulk insert",
			data: []map[string]any{
				{"id": 1, "name": "Test1"},
				{"id": 2, "name": "Test2"},
				{"id": 3, "name": "Test3"},
			},
			expectBulk:  true,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock.ExpectBegin()

			if tt.expectBulk {
				mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectPrepare("COPY")
				for range tt.data {
					mock.ExpectExec("COPY").WillReturnResult(sqlmock.NewResult(0, 1))
				}
				mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, int64(len(tt.data))))
			} else {
				mock.ExpectExec("INSERT INTO").
					WithArgs(tt.data[0]["id"], tt.data[0]["name"]).
					WillReturnResult(sqlmock.NewResult(0, 1))
			}

			mock.ExpectCommit()

			result, err := plugin.TableCreate("test_user", "test_table", tt.data, nil)
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(result) != len(tt.data) {
				t.Errorf("Expected %d results, got %d", len(tt.data), len(result))
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

func TestGetSchemaComprehensive(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	defer db.Close()

	plugin := &pgPlugin{
		db: db,
	}

	// Mock context application
	mock.ExpectBegin()
	mock.ExpectExec("SELECT set_config").WithArgs("request.user", "test_user").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("SELECT set_config").WithArgs("erctx.user", "test_user").WillReturnResult(sqlmock.NewResult(0, 1))

	// Mock tables query
	tableRows := sqlmock.NewRows([]string{"table_name"}).
		AddRow("users").
		AddRow("products")
	mock.ExpectQuery("SELECT table_name FROM information_schema.tables").WillReturnRows(tableRows)

	// Mock columns query for users table
	userColumns := sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
		AddRow("id", "integer", "NO", nil).
		AddRow("name", "character varying", "NO", nil).
		AddRow("created_at", "timestamp", "YES", "CURRENT_TIMESTAMP")
	mock.ExpectQuery("SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns").
		WithArgs("users").
		WillReturnRows(userColumns)

	// Mock columns query for products table
	productColumns := sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
		AddRow("id", "integer", "NO", nil).
		AddRow("name", "character varying", "NO", nil).
		AddRow("price", "numeric", "NO", nil)
	mock.ExpectQuery("SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns").
		WithArgs("products").
		WillReturnRows(productColumns)

	// Mock views query
	viewRows := sqlmock.NewRows([]string{"table_name"}).
		AddRow("user_stats")
	mock.ExpectQuery("SELECT table_name FROM information_schema.tables.*VIEW").WillReturnRows(viewRows)

	// Mock columns query for view
	viewColumns := sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default"}).
		AddRow("user_id", "integer", "YES", nil).
		AddRow("total_orders", "bigint", "YES", nil)
	mock.ExpectQuery("SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns").
		WithArgs("user_stats").
		WillReturnRows(viewColumns)

	// Mock functions query
	functionRows := sqlmock.NewRows([]string{"routine_name", "specific_name", "data_type"}).
		AddRow("calculate_total", "calculate_total_123", "numeric")
	mock.ExpectQuery("SELECT routine_name, specific_name, data_type FROM information_schema.routines").
		WillReturnRows(functionRows)

	// Mock function parameters query
	paramRows := sqlmock.NewRows([]string{"parameter_name", "data_type", "parameter_mode", "ordinal_position"}).
		AddRow("user_id", "integer", "IN", 1).
		AddRow("start_date", "date", "IN", 2)
	mock.ExpectQuery("SELECT parameter_name, data_type, parameter_mode, ordinal_position FROM information_schema.parameters").
		WithArgs("calculate_total_123").
		WillReturnRows(paramRows)

	mock.ExpectCommit()

	ctx := map[string]any{
		"user": "test_user",
	}

	schema, err := plugin.GetSchema(ctx)
	if err != nil {
		t.Fatalf("GetSchema failed: %v", err)
	}

	schemaMap, ok := schema.(map[string]any)
	if !ok {
		t.Fatal("Schema is not a map")
	}

	// Verify tables
	tables, ok := schemaMap["tables"].(map[string]any)
	if !ok {
		t.Fatal("Tables schema is not a map")
	}

	// Check users table
	usersSchema, ok := tables["users"].(map[string]any)
	if !ok {
		t.Fatal("Users table schema is not a map")
	}

	usersProps, ok := usersSchema["properties"].(map[string]any)
	if !ok {
		t.Fatal("Users properties is not a map")
	}

	if _, ok := usersProps["id"]; !ok {
		t.Error("Users schema missing id field")
	}

	// Check views
	views, ok := schemaMap["views"].(map[string]any)
	if !ok {
		t.Fatal("Views schema is not a map")
	}

	if _, ok := views["user_stats"]; !ok {
		t.Error("Views schema missing user_stats view")
	}

	// Check RPC
	rpc, ok := schemaMap["rpc"].(map[string]any)
	if !ok {
		t.Fatal("RPC schema is not a map")
	}

	if _, ok := rpc["calculate_total"]; !ok {
		t.Error("RPC schema missing calculate_total function")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled expectations: %v", err)
	}
}

func TestTableDeleteComprehensive(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("Failed to create mock: %v", err)
	}
	defer db.Close()

	plugin := &pgPlugin{
		db: db,
	}

	tests := []struct {
		name        string
		table       string
		where       map[string]any
		ctx         map[string]any
		affected    int64
		expectError bool
	}{
		{
			name:        "Simple delete",
			table:       "users",
			where:       map[string]any{"id": 1},
			ctx:         nil,
			affected:    1,
			expectError: false,
		},
		{
			name:        "Delete with context",
			table:       "orders",
			where:       map[string]any{"status": "pending"},
			ctx:         map[string]any{"user_id": 123},
			affected:    2,
			expectError: false,
		},
		{
			name:        "Delete all",
			table:       "temp_data",
			where:       map[string]any{},
			ctx:         nil,
			affected:    5,
			expectError: false,
		},
		{
			name:        "Delete with multiple conditions",
			table:       "products",
			where:       map[string]any{"category": "electronics", "price": 100},
			ctx:         nil,
			affected:    3,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock.ExpectBegin()

			if tt.ctx != nil {
				for k, v := range tt.ctx {
					mock.ExpectExec("SELECT set_config").
						WithArgs("request."+k, fmt.Sprintf("%v", v)).
						WillReturnResult(sqlmock.NewResult(0, 1))
					mock.ExpectExec("SELECT set_config").
						WithArgs("erctx."+k, fmt.Sprintf("%v", v)).
						WillReturnResult(sqlmock.NewResult(0, 1))
				}
			}

			whereClause := "DELETE FROM " + tt.table
			if len(tt.where) > 0 {
				whereClause += " WHERE"
			}

			mock.ExpectExec(whereClause).WillReturnResult(sqlmock.NewResult(0, tt.affected))
			mock.ExpectCommit()

			affected, err := plugin.TableDelete("test_user", tt.table, tt.where, tt.ctx)
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if int64(affected) != tt.affected {
				t.Errorf("Expected %d affected rows, got %d", tt.affected, affected)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled expectations: %v", err)
			}
		})
	}
}

// newTestPluginWithCache creates a pgPlugin, pgCachePlugin, and sqlmock instance for testing.
func newTestPluginWithCache(t *testing.T) (*pgPlugin, *pgCachePlugin, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp)) // Use Regexp matcher
	if err != nil {
		t.Fatalf("failed to open sqlmock database: %s", err)
	}
	// Create the main plugin instance with the mock DB
	plugin := &pgPlugin{db: db}
	// Create the cache plugin instance, pointing to the main plugin
	cachePlugin := &pgCachePlugin{dbPluginPointer: plugin}
	return plugin, cachePlugin, mock
}

func TestPgCachePluginInitConnection(t *testing.T) {
	// 1. Setup: Create plugin instances and the mock DB connection
	plugin, cachePlugin, mock := newTestPluginWithCache(t)
	// Ensure the mock DB is closed after the test
	defer plugin.db.Close()

	// 2. Expectation: Mock the SQL query that InitConnection should execute
	// It expects "CREATE TABLE IF NOT EXISTS easyrest_cache ..."
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS easyrest_cache").
		WillReturnResult(sqlmock.NewResult(0, 0)) // Expect success (no rows affected is fine for CREATE TABLE)

	// 3. Action: Call the InitConnection method on the cache plugin instance
	// Provide a dummy URI; in the test, the DB connection comes from the mock setup,
	// but InitConnection might internally call the main plugin's InitConnection if db is nil.
	err := cachePlugin.InitConnection("postgres://user:pass@localhost:5432/db")

	// 4. Assertion: Check if there was an error during initialization
	if err != nil {
		t.Fatalf("cachePlugin.InitConnection failed: %v", err)
	}

	// 5. Verification: Ensure that the expected SQL query was actually executed
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestPgCachePluginInitConnection: %s", err)
	}
	// Note: Testing the background cleanup goroutine directly is complex in unit tests.
	// We assume it's started if InitConnection succeeds without error.
}

func TestPgCachePluginSetGet(t *testing.T) {
	plugin, cachePlugin, mock := newTestPluginWithCache(t)
	defer plugin.db.Close()

	key := "testKey"
	value := "testValue"
	ttl := 5 * time.Minute
	expiresAt := time.Now().Add(ttl) // Approximate expected time

	// --- Test Set ---
	// Mock the INSERT ... ON CONFLICT query called by the Set method
	// Use sqlmock.AnyArg() for expiresAt, as the exact time might differ slightly
	mock.ExpectExec("INSERT INTO easyrest_cache .* ON CONFLICT").
		WithArgs(key, value, sqlmock.AnyArg()).   // Check key, value, and any timestamp
		WillReturnResult(sqlmock.NewResult(1, 1)) // Expect successful insertion/update of 1 row

	// Call the Set method
	err := cachePlugin.Set(key, value, ttl)
	if err != nil {
		t.Fatalf("cachePlugin.Set failed: %v", err)
	}

	// --- Test Get (successful) ---
	// Mock the SELECT query called by the Get method
	rows := sqlmock.NewRows([]string{"value"}).AddRow(value) // Prepare result rows
	mock.ExpectQuery("SELECT value FROM easyrest_cache WHERE key = \\$1 AND expires_at > NOW()").
		WithArgs(key).       // Check that the query is executed with the correct key
		WillReturnRows(rows) // Return the prepared rows

	// Call the Get method
	retrievedValue, err := cachePlugin.Get(key)
	if err != nil {
		t.Fatalf("cachePlugin.Get failed: %v", err)
	}

	// Check that the retrieved value matches the expected value
	if retrievedValue != value {
		t.Errorf("expected value '%s', got '%s'", value, retrievedValue)
	}

	// Check that all mock expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestPgCachePluginSetGet: %s", err)
	}

	// --- Test Get (key not found) ---
	// Mock the SELECT query for a non-existent key
	mock.ExpectQuery("SELECT value FROM easyrest_cache WHERE key = \\$1 AND expires_at > NOW()").
		WithArgs("nonExistentKey").
		WillReturnError(sql.ErrNoRows) // Expect sql.ErrNoRows error (standard for cache miss)

	// Call Get for the non-existent key
	_, err = cachePlugin.Get("nonExistentKey")
	// Check that the specific sql.ErrNoRows error was returned
	if !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("expected sql.ErrNoRows for non-existent key, got %v", err)
	}

	// Check mock expectations again
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations after non-existent key check: %s", err)
	}

	// Optional: More complex validation of the exact expiresAt time could be added,
	// but it would require extracting the argument from sqlmock.AnyArg().
	_ = expiresAt // Use expiresAt to avoid "unused variable" error
}

func TestPgCachePluginExpiration(t *testing.T) {
	plugin, cachePlugin, mock := newTestPluginWithCache(t)
	defer plugin.db.Close()

	key := "expiredKey"

	// --- Test Get (key expired) ---
	// Mock the SELECT query for a key considered expired
	// Mock returns sql.ErrNoRows because the 'expires_at > NOW()' condition would fail
	mock.ExpectQuery("SELECT value FROM easyrest_cache WHERE key = \\$1 AND expires_at > NOW()").
		WithArgs(key).
		WillReturnError(sql.ErrNoRows)

	// Call Get for the "expired" key
	_, err := cachePlugin.Get(key)
	// Check that sql.ErrNoRows error was returned
	if !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("expected sql.ErrNoRows for expired key, got %v", err)
	}

	// Check that all mock expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestPgCachePluginExpiration: %s", err)
	}
}
