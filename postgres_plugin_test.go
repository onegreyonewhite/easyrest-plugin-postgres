// pg_plugin_test.go
package main

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func newTestPlugin(t *testing.T) (*pgPlugin, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("failed to open sqlmock database: %s", err)
	}
	plugin := &pgPlugin{db: db}
	return plugin, mock
}

func TestInitConnectionValid(t *testing.T) {
	plugin := &pgPlugin{}
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp), sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("failed to open sqlmock database: %s", err)
	}
	defer db.Close()
	plugin.db = db

	mock.ExpectPing().WillReturnError(nil)
	if err := plugin.db.Ping(); err != nil {
		t.Fatalf("Ping failed: %s", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestInitConnectionValid: %s", err)
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
	where := map[string]interface{}{
		"status": map[string]interface{}{"=": "active"},
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
	ctx := map[string]interface{}{
		"claims_sub": "sub_value",
	}
	selectFields := []string{"id", "erctx.claims_sub"}
	where := map[string]interface{}{}
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
	where := map[string]interface{}{"dummy": "val"}
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
	ctxData := map[string]interface{}{
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
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()
	data := []map[string]interface{}{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
		{"id": 3, "name": "Charlie"},
	}
	expectedRegex := regexp.QuoteMeta("INSERT INTO users (id, name) VALUES ($1, $2), ($3, $4), ($5, $6)")
	mock.ExpectBegin()
	mock.ExpectExec(expectedRegex).
		WithArgs(1, "Alice", 2, "Bob", 3, "Charlie").
		WillReturnResult(sqlmock.NewResult(1, 3))
	mock.ExpectCommit()
	result, err := plugin.TableCreate("user1", "users", data, nil)
	if err != nil {
		t.Fatalf("TableCreate error: %s", err)
	}
	if len(result) != 3 {
		t.Errorf("expected 3 rows inserted, got %d", len(result))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestTableCreate: %s", err)
	}
}

func TestTableUpdate(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()
	data := map[string]interface{}{
		"name": "Bob",
	}
	where := map[string]interface{}{
		"id": map[string]interface{}{"=": 1},
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
	where := map[string]interface{}{
		"status": map[string]interface{}{"=": "inactive"},
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
	data := map[string]interface{}{
		"param": "value",
	}
	mock.ExpectBegin()
	queryRegex := regexp.QuoteMeta(`SELECT doSomething($1) AS result`)
	rows := sqlmock.NewRows([]string{"result"}).AddRow("success")
	mock.ExpectQuery(queryRegex).WithArgs("value").WillReturnRows(rows)
	mock.ExpectCommit()
	result, err := plugin.CallFunction("user1", "doSomething", data, nil)
	if err != nil {
		t.Fatalf("CallFunction error: %s", err)
	}
	out, _ := json.Marshal(result)
	var res interface{}
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("failed to unmarshal result: %s", err)
	}
	if res != "success" {
		t.Errorf("expected 'success', got %v", res)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestCallFunction: %s", err)
	}
}

func TestGetSchema(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()
	ctxData := map[string]interface{}{
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
