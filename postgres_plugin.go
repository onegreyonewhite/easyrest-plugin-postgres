// postgres_plugin.go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	hplugin "github.com/hashicorp/go-plugin"
	_ "github.com/lib/pq"
	easyrest "github.com/onegreyonewhite/easyrest/plugin"
)

var Version = "v0.2.0"

// pgPlugin implements the DBPlugin interface for PostgreSQL.
type pgPlugin struct {
	db *sql.DB
}

// InitConnection opens a PostgreSQL connection using a URI with the prefix "postgres://".
func (p *pgPlugin) InitConnection(uri string) error {
	if !strings.HasPrefix(uri, "postgres://") {
		return errors.New("invalid postgres URI")
	}
	dsn := "postgres://" + strings.TrimPrefix(uri, "postgres://")
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.Ping(); err != nil {
		return err
	}
	p.db = db
	return nil
}

// ApplyPluginContext sets session variables using set_config for all keys in ctx.
// For each key, it sets two variables: one with prefix "request." (complex types are marshaled to JSON)
// and one with prefix "erctx." using the default string representation.
// Keys are processed in sorted order.
func ApplyPluginContext(tx *sql.Tx, ctx map[string]interface{}) error {
	// Collect and sort keys.
	keys := make([]string, 0, len(ctx))
	for k := range ctx {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		val := ctx[key]
		var requestVal string
		switch val.(type) {
		case map[string]interface{}, []interface{}:
			b, err := json.Marshal(val)
			if err != nil {
				return err
			}
			requestVal = string(b)
		default:
			requestVal = fmt.Sprintf("%v", val)
		}
		erctxVal := fmt.Sprintf("%v", val)
		query := "SELECT set_config($1, $2, true)"
		if _, err := tx.ExecContext(context.Background(), query, "request."+key, requestVal); err != nil {
			return err
		}
		if _, err := tx.ExecContext(context.Background(), query, "erctx."+key, erctxVal); err != nil {
			return err
		}
	}
	return nil
}

// TableGet executes a SELECT query with optional WHERE, GROUP BY, ORDER BY, LIMIT and OFFSET.
func (p *pgPlugin) TableGet(userID, table string, selectFields []string, where map[string]interface{},
	ordering []string, groupBy []string, limit, offset int, ctx map[string]interface{}) ([]map[string]interface{}, error) {

	fields := "*"
	if len(selectFields) > 0 {
		fields = strings.Join(selectFields, ", ")
	}
	query := fmt.Sprintf("SELECT %s FROM %s", fields, table)
	whereClause, args, err := easyrest.BuildWhereClause(where)
	if err != nil {
		return nil, err
	}
	query += whereClause
	if len(groupBy) > 0 {
		query += " GROUP BY " + strings.Join(groupBy, ", ")
	}
	if len(ordering) > 0 {
		query += " ORDER BY " + strings.Join(ordering, ", ")
	}
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", offset)
	}
	query = convertPlaceholders(query)

	tx, err := p.db.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	loc := time.UTC
	if ctx != nil {
		if err := ApplyPluginContext(tx, ctx); err != nil {
			tx.Rollback()
			return nil, err
		}
		if tz, ok := ctx["timezone"].(string); ok && tz != "" {
			if l, err := time.LoadLocation(tz); err == nil {
				loc = l
			}
		}
	}

	rows, err := tx.QueryContext(context.Background(), query, args...)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		tx.Rollback()
		return nil, err
	}
	numCols := len(cols)
	var results []map[string]interface{}
	for rows.Next() {
		columns := make([]interface{}, numCols)
		columnPointers := make([]interface{}, numCols)
		for i := range columns {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			rows.Close()
			tx.Rollback()
			return nil, err
		}
		rowMap := make(map[string]interface{}, numCols)
		for i, colName := range cols {
			if t, ok := columns[i].(time.Time); ok {
				adjusted := t.In(loc)
				if adjusted.Hour() == 0 && adjusted.Minute() == 0 && adjusted.Second() == 0 && adjusted.Nanosecond() == 0 {
					rowMap[colName] = adjusted.Format("2006-01-02")
				} else {
					rowMap[colName] = adjusted.Format("2006-01-02 15:04:05")
				}
			} else {
				rowMap[colName] = columns[i]
			}
		}
		results = append(results, rowMap)
	}
	rows.Close() // Close rows synchronously
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return results, nil
}

// TableCreate performs an INSERT operation on the specified table
func (p *pgPlugin) TableCreate(userID, table string, data []map[string]interface{}, ctx map[string]interface{}) ([]map[string]interface{}, error) {
	if len(data) == 0 {
		return []map[string]interface{}{}, nil
	}
	tx, err := p.db.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	if ctx != nil {
		if err := ApplyPluginContext(tx, ctx); err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	keys := make([]string, 0, len(data[0]))
	for k := range data[0] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	colsStr := strings.Join(keys, ", ")

	var valuePlaceholders []string
	var args []interface{}
	for _, row := range data {
		var ph []string
		for _, k := range keys {
			ph = append(ph, "?")
			args = append(args, row[k])
		}
		valuePlaceholders = append(valuePlaceholders, "("+strings.Join(ph, ", ")+")")
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, colsStr, strings.Join(valuePlaceholders, ", "))
	query = convertPlaceholders(query)
	if _, err := tx.ExecContext(context.Background(), query, args...); err != nil {
		tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return data, nil
}

// TableUpdate executes an UPDATE query on the specified table.
func (p *pgPlugin) TableUpdate(userID, table string, data map[string]interface{}, where map[string]interface{}, ctx map[string]interface{}) (int, error) {
	tx, err := p.db.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, err
	}
	if ctx != nil {
		if err := ApplyPluginContext(tx, ctx); err != nil {
			tx.Rollback()
			return 0, err
		}
	}
	var setParts []string
	var args []interface{}
	for k, v := range data {
		setParts = append(setParts, fmt.Sprintf("%s = ?", k))
		args = append(args, v)
	}
	query := fmt.Sprintf("UPDATE %s SET %s", table, strings.Join(setParts, ", "))
	whereClause, whereArgs, err := easyrest.BuildWhereClause(where)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	query += whereClause
	args = append(args, whereArgs...)
	query = convertPlaceholders(query)
	res, err := tx.ExecContext(context.Background(), query, args...)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return int(affected), nil
}

// TableDelete executes a DELETE query on the specified table.
func (p *pgPlugin) TableDelete(userID, table string, where map[string]interface{}, ctx map[string]interface{}) (int, error) {
	tx, err := p.db.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, err
	}
	if ctx != nil {
		if err := ApplyPluginContext(tx, ctx); err != nil {
			tx.Rollback()
			return 0, err
		}
	}
	whereClause, args, err := easyrest.BuildWhereClause(where)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	query := fmt.Sprintf("DELETE FROM %s%s", table, whereClause)
	query = convertPlaceholders(query)
	res, err := tx.ExecContext(context.Background(), query, args...)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return int(affected), nil
}

// CallFunction executes a function call using SELECT syntax.
// Parameters are passed in sorted order by key.
func (p *pgPlugin) CallFunction(userID, funcName string, data map[string]interface{}, ctx map[string]interface{}) (interface{}, error) {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var placeholders []string
	var args []interface{}
	for _, k := range keys {
		placeholders = append(placeholders, "?")
		args = append(args, data[k])
	}
	query := fmt.Sprintf("SELECT %s(%s) AS result", funcName, strings.Join(placeholders, ", "))
	query = convertPlaceholders(query)
	tx, err := p.db.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	if ctx != nil {
		if err := ApplyPluginContext(tx, ctx); err != nil {
			tx.Rollback()
			return nil, err
		}
	}
	rows, err := tx.QueryContext(context.Background(), query, args...)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		tx.Rollback()
		return nil, err
	}
	var result interface{}
	if rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			rows.Close()
			tx.Rollback()
			return nil, err
		}
		if len(columns) > 0 {
			result = columns[0]
		}
	}
	rows.Close() // Synchronously close rows
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

// convertPlaceholders replaces each "?" in the query with PostgreSQL-style "$1", "$2", etc.
func convertPlaceholders(query string) string {
	var builder strings.Builder
	counter := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			builder.WriteString(fmt.Sprintf("$%d", counter))
			counter++
		} else {
			builder.WriteByte(query[i])
		}
	}
	return builder.String()
}

// getTablesSchema builds a schema for each base table from information_schema.
func (p *pgPlugin) getTablesSchema(tx *sql.Tx) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	tableQuery := "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
	rows, err := tx.QueryContext(context.Background(), tableQuery)
	if err != nil {
		return nil, err
	}
	var tableNames []string
	for rows.Next() {
		var tn string
		if err := rows.Scan(&tn); err != nil {
			rows.Close()
			return nil, err
		}
		tableNames = append(tableNames, tn)
	}
	rows.Close()
	for _, tn := range tableNames {
		colQuery := "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1"
		colRows, err := tx.QueryContext(context.Background(), colQuery, tn)
		if err != nil {
			return nil, err
		}
		properties := make(map[string]interface{})
		var required []string
		for colRows.Next() {
			var colName, dataType, isNullable string
			var colDefault sql.NullString
			if err := colRows.Scan(&colName, &dataType, &isNullable, &colDefault); err != nil {
				colRows.Close()
				return nil, err
			}
			swType := mapPostgresType(dataType)
			prop := map[string]interface{}{
				"type": swType,
			}
			if isNullable == "YES" {
				prop["x-nullable"] = true
			}
			if isNullable == "NO" && !colDefault.Valid {
				required = append(required, colName)
			}
			properties[colName] = prop
		}
		colRows.Close()
		schema := map[string]interface{}{
			"type":       "object",
			"properties": properties,
		}
		if len(required) > 0 {
			schema["required"] = required
		}
		result[tn] = schema
	}
	return result, nil
}

// getViewsSchema builds a schema for each view from information_schema.
func (p *pgPlugin) getViewsSchema(tx *sql.Tx) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'VIEW'"
	rows, err := tx.QueryContext(context.Background(), query)
	if err != nil {
		return nil, err
	}
	var viewNames []string
	for rows.Next() {
		var vn string
		if err := rows.Scan(&vn); err != nil {
			rows.Close()
			return nil, err
		}
		viewNames = append(viewNames, vn)
	}
	rows.Close()
	for _, vn := range viewNames {
		colQuery := "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1"
		colRows, err := tx.QueryContext(context.Background(), colQuery, vn)
		if err != nil {
			return nil, err
		}
		properties := make(map[string]interface{})
		var required []string
		for colRows.Next() {
			var colName, dataType, isNullable string
			var colDefault sql.NullString
			if err := colRows.Scan(&colName, &dataType, &isNullable, &colDefault); err != nil {
				colRows.Close()
				return nil, err
			}
			swType := mapPostgresType(dataType)
			prop := map[string]interface{}{
				"type": swType,
			}
			if isNullable == "YES" {
				prop["x-nullable"] = true
			}
			if isNullable == "NO" && !colDefault.Valid {
				required = append(required, colName)
			}
			properties[colName] = prop
		}
		colRows.Close()
		schema := map[string]interface{}{
			"type":       "object",
			"properties": properties,
		}
		if len(required) > 0 {
			schema["required"] = required
		}
		result[vn] = schema
	}
	return result, nil
}

// getRPCSchema builds schemas for stored functions.
func (p *pgPlugin) getRPCSchema(tx *sql.Tx) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	rpcQuery := "SELECT routine_name, specific_name, data_type FROM information_schema.routines WHERE specific_schema = 'public' AND routine_type = 'FUNCTION'"
	rows, err := tx.QueryContext(context.Background(), rpcQuery)
	if err != nil {
		return nil, err
	}
	var routines []struct {
		routineName  string
		specificName string
		returnType   string
	}
	for rows.Next() {
		var rn, sn, rt string
		if err := rows.Scan(&rn, &sn, &rt); err != nil {
			rows.Close()
			return nil, err
		}
		routines = append(routines, struct {
			routineName  string
			specificName string
			returnType   string
		}{rn, sn, rt})
	}
	rows.Close()
	for _, r := range routines {
		paramQuery := "SELECT parameter_name, data_type, parameter_mode, ordinal_position FROM information_schema.parameters WHERE specific_schema = 'public' AND specific_name = $1 ORDER BY ordinal_position"
		paramRows, err := tx.QueryContext(context.Background(), paramQuery, r.specificName)
		if err != nil {
			return nil, err
		}
		properties := make(map[string]interface{})
		var reqFields []string
		for paramRows.Next() {
			var paramName sql.NullString
			var dataType, paramMode string
			var ordinal int
			if err := paramRows.Scan(&paramName, &dataType, &paramMode, &ordinal); err != nil {
				paramRows.Close()
				return nil, err
			}
			if !paramName.Valid {
				continue
			}
			swType := mapPostgresType(dataType)
			prop := map[string]interface{}{
				"type": swType,
			}
			properties[paramName.String] = prop
			reqFields = append(reqFields, paramName.String)
		}
		paramRows.Close()
		inSchema := map[string]interface{}{
			"type":       "object",
			"properties": properties,
		}
		if len(reqFields) > 0 {
			inSchema["required"] = reqFields
		}
		outSchema := map[string]interface{}{
			"type":       "object",
			"properties": map[string]interface{}{},
		}
		if strings.ToLower(r.returnType) != "void" {
			outSchema["properties"] = map[string]interface{}{
				"result": map[string]interface{}{
					"type": mapPostgresType(r.returnType),
				},
			}
		}
		result[r.routineName] = []interface{}{inSchema, outSchema}
	}
	return result, nil
}

// mapPostgresType converts a PostgreSQL data type to a JSON schema type.
func mapPostgresType(dt string) string {
	up := strings.ToUpper(dt)
	if strings.Contains(up, "INT") {
		return "integer"
	}
	if strings.Contains(up, "CHAR") || strings.Contains(up, "TEXT") {
		return "string"
	}
	if strings.Contains(up, "BOOL") {
		return "boolean"
	}
	if strings.Contains(up, "DATE") {
		return "string"
	}
	if strings.Contains(up, "TIME") {
		return "string"
	}
	if strings.Contains(up, "NUMERIC") || strings.Contains(up, "DECIMAL") || strings.Contains(up, "REAL") || strings.Contains(up, "DOUBLE") {
		return "number"
	}
	return "string"
}

// GetSchema returns a schema object with "tables", "views" and "rpc" keys.
// It applies the provided context to the session before retrieving the schema.
func (p *pgPlugin) GetSchema(ctx map[string]interface{}) (interface{}, error) {
	tx, err := p.db.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	if ctx != nil {
		if err := ApplyPluginContext(tx, ctx); err != nil {
			tx.Rollback()
			return nil, err
		}
	}
	tables, err := p.getTablesSchema(tx)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	views, err := p.getViewsSchema(tx)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	rpc, err := p.getRPCSchema(tx)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"tables": tables,
		"views":  views,
		"rpc":    rpc,
	}, nil
}

func main() {
	showVersion := flag.Bool("version", false, "Show version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(Version)
		return
	}
	impl := &pgPlugin{}
	hplugin.Serve(&hplugin.ServeConfig{
		HandshakeConfig: easyrest.Handshake,
		Plugins: map[string]hplugin.Plugin{
			"db": &easyrest.DBPluginPlugin{Impl: impl},
		},
	})
}
