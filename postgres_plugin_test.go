// pg_plugin_test.go
package main

import (
	"encoding/json"
	"regexp"
	"testing"

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
	db, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
		sqlmock.MonitorPingsOption(true),
	)
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

func TestSubstituteContextValues(t *testing.T) {
	flatCtx := map[string]string{
		"token": "secret123",
	}
	input := map[string]interface{}{
		"auth": "erctx.token",
		"msg":  "hello",
	}
	result := substituteContextValues(input, flatCtx)
	resMap, ok := result.(map[string]interface{})
	if !ok {
		t.Fatal("expected map[string]interface{}")
	}
	if resMap["auth"] != "secret123" {
		t.Errorf("expected 'secret123', got %v", resMap["auth"])
	}
	if resMap["msg"] != "hello" {
		t.Errorf("expected 'hello', got %v", resMap["msg"])
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

	// Set context with a value for "claims_sub".
	ctx := map[string]interface{}{
		"claims_sub": "sub_value",
	}
	// selectFields includes a regular field and an erctx field.
	selectFields := []string{"id", "erctx.claims_sub"}
	where := map[string]interface{}{}

	// Expect transaction begin.
	mock.ExpectBegin()
	// Expect two set_config calls for the context.
	mock.ExpectExec(regexp.QuoteMeta("SELECT set_config($1, $2, true)")).
		WithArgs("request.claims_sub", "sub_value").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("SELECT set_config($1, $2, true)")).
		WithArgs("erctx.claims_sub", "sub_value").
		WillReturnResult(sqlmock.NewResult(1, 1))
	// The select clause should substitute "erctx.claims_sub" with a placeholder.
	expectedQueryRegex := regexp.QuoteMeta("SELECT id, $1 AS erctx.claims_sub FROM users")
	rows := sqlmock.NewRows([]string{"id", "erctx.claims_sub"}).AddRow(10, "sub_value")
	// Expect query with the substituted select field.
	mock.ExpectQuery(expectedQueryRegex).WithArgs("sub_value").WillReturnRows(rows)
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

func TestTableCreate(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	data := []map[string]interface{}{
		{"id": 1, "name": "Alice"},
	}
	mock.ExpectBegin()
	// Since ctx == nil, no extra set_config calls are made.
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO users (`)).
		WithArgs(1, "Alice").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	result, err := plugin.TableCreate("user1", "users", data, nil)
	if err != nil {
		t.Fatalf("TableCreate error: %s", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 row inserted, got %d", len(result))
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

func TestApplyPluginContext(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	// Expect transaction begin.
	mock.ExpectBegin()
	tx, err := plugin.db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %s", err)
	}

	ctx := map[string]interface{}{
		"timezone": "UTC",
		"method":   "GET",
		"claims":   map[string]interface{}{"user": "alice"},
	}
	// Expect two set_config calls for each key.
	mock.ExpectExec(regexp.QuoteMeta("SELECT set_config($1, $2, true)")).
		WithArgs("request.timezone", "UTC").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("SELECT set_config($1, $2, true)")).
		WithArgs("erctx.timezone", "UTC").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("SELECT set_config($1, $2, true)")).
		WithArgs("request.method", "GET").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("SELECT set_config($1, $2, true)")).
		WithArgs("erctx.method", "GET").
		WillReturnResult(sqlmock.NewResult(1, 1))
	claimsJSON, _ := json.Marshal(map[string]interface{}{"user": "alice"})
	mock.ExpectExec(regexp.QuoteMeta("SELECT set_config($1, $2, true)")).
		WithArgs("request.claims", string(claimsJSON)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("SELECT set_config($1, $2, true)")).
		WithArgs("erctx.claims", string(claimsJSON)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Expect transaction commit.
	mock.ExpectCommit()

	if err := ApplyPluginContext(tx, ctx); err != nil {
		tx.Rollback()
		t.Fatalf("ApplyPluginContext error: %s", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %s", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestApplyPluginContext: %s", err)
	}
}
