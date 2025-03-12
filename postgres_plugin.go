// postgres_plugin.go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	hplugin "github.com/hashicorp/go-plugin"
	_ "github.com/lib/pq"
	easyrest "github.com/onegreyonewhite/easyrest/plugin"
)

var Version = "v0.1.0"

// pgPlugin implements the DBPlugin interface for PostgreSQL.
type pgPlugin struct {
	db *sql.DB
}

// InitConnection opens a PostgreSQL connection using a URI with the prefix "postgres://".
func (p *pgPlugin) InitConnection(uri string) error {
	// Validate URI prefix and convert to PostgreSQL DSN.
	if !strings.HasPrefix(uri, "postgres://") {
		return errors.New("invalid postgres URI")
	}
	dsn := "postgres://" + strings.TrimPrefix(uri, "postgres://")
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	// Set connection pool parameters.
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.Ping(); err != nil {
		return err
	}
	p.db = db
	return nil
}

// ApplyPluginContext sets context variables in PostgreSQL using set_config.
// For each key in ctx, two settings are applied: "request.<key>" and "erctx.<key>".
// If the value is a map, slice, or array, it is serialized to JSON.
func ApplyPluginContext(tx *sql.Tx, ctx map[string]interface{}) error {
	for key, val := range ctx {
		var valStr string
		switch v := val.(type) {
		case string:
			valStr = v
		case int, int32, int64, float32, float64, bool:
			valStr = fmt.Sprintf("%v", v)
		default:
			// Serialize complex types to JSON.
			b, err := json.Marshal(v)
			if err != nil {
				return err
			}
			valStr = string(b)
		}
		// Set for request.<key>
		query := "SELECT set_config($1, $2, true)"
		requestKey := "request." + key
		if _, err := tx.ExecContext(context.Background(), query, requestKey, valStr); err != nil {
			return err
		}
		// Set for erctx.<key>
		erctxKey := "erctx." + key
		if _, err := tx.ExecContext(context.Background(), query, erctxKey, valStr); err != nil {
			return err
		}
	}
	return nil
}

// TableGet executes a SELECT query with optional WHERE, GROUP BY, ORDER BY, LIMIT, and OFFSET.
// It now also processes selectFields: any field starting with "erctx." is replaced by a literal
// value from the context (if available) using a placeholder and alias.
func (p *pgPlugin) TableGet(userID, table string, selectFields []string, where map[string]interface{},
	ordering []string, groupBy []string, limit, offset int, ctx map[string]interface{}) ([]map[string]interface{}, error) {

	var flatCtx map[string]string
	var err error
	var selectArgs []interface{}
	if ctx != nil {
		flatCtx, err = easyrest.FormatToContext(ctx)
		if err != nil {
			return nil, err
		}
		// Substitute context values in the WHERE clause.
		substituted := substituteContextValues(where, flatCtx)
		if m, ok := substituted.(map[string]interface{}); ok {
			where = m
		} else {
			return nil, fmt.Errorf("expected map[string]interface{} after substitution")
		}
	}
	var fields string
	if len(selectFields) > 0 {
		newSelectFields := make([]string, 0, len(selectFields))
		for _, field := range selectFields {
			// If the field begins with "erctx." and context has a corresponding value,
			// replace the field with a literal placeholder and alias.
			if flatCtx != nil && strings.HasPrefix(field, "erctx.") {
				key := strings.TrimPrefix(field, "erctx.")
				normalizedKey := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
				if val, exists := flatCtx[normalizedKey]; exists {
					newSelectFields = append(newSelectFields, fmt.Sprintf("? AS %s", field))
					selectArgs = append(selectArgs, val)
					continue
				}
			}
			newSelectFields = append(newSelectFields, field)
		}
		fields = strings.Join(newSelectFields, ", ")
	} else {
		fields = "*"
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
	// Convert "?" placeholders to PostgreSQL format.
	query = convertPlaceholders(query)
	combinedArgs := append(selectArgs, args...)
	log.Println(query)
	log.Println(combinedArgs)

	// Begin a transaction to apply context variables.
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
	rows, err := tx.QueryContext(context.Background(), query, combinedArgs...)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
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
			tx.Rollback()
			return nil, err
		}
		rowMap := make(map[string]interface{}, numCols)
		for i, colName := range cols {
			// Format time values appropriately.
			if t, ok := columns[i].(time.Time); ok {
				if t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0 {
					rowMap[colName] = t.Format("2006-01-02")
				} else {
					rowMap[colName] = t.Format("2006-01-02 15:04:05")
				}
			} else {
				rowMap[colName] = columns[i]
			}
		}
		results = append(results, rowMap)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return results, nil
}

// TableCreate performs an INSERT operation on the specified table.
func (p *pgPlugin) TableCreate(userID, table string, data []map[string]interface{}, ctx map[string]interface{}) ([]map[string]interface{}, error) {
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
	var results []map[string]interface{}
	var flatCtx map[string]string
	if ctx != nil {
		flatCtx, err = easyrest.FormatToContext(ctx)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
	}
	// Process each row for insertion.
	for _, row := range data {
		substituted := substituteContextValues(row, flatCtx)
		rowMap, ok := substituted.(map[string]interface{})
		if !ok {
			tx.Rollback()
			return nil, fmt.Errorf("failed to substitute context in row")
		}
		// Sort keys for deterministic order.
		keys := make([]string, 0, len(rowMap))
		for k := range rowMap {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var cols []string
		var placeholders []string
		var args []interface{}
		for _, k := range keys {
			cols = append(cols, k)
			placeholders = append(placeholders, "?")
			args = append(args, rowMap[k])
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ", "), strings.Join(placeholders, ", "))
		query = convertPlaceholders(query)
		_, err := tx.ExecContext(context.Background(), query, args...)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		results = append(results, rowMap)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return results, nil
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
	var flatCtx map[string]string
	if ctx != nil {
		flatCtx, err = easyrest.FormatToContext(ctx)
		if err != nil {
			tx.Rollback()
			return 0, err
		}
	}
	substitutedData, ok := substituteContextValues(data, flatCtx).(map[string]interface{})
	if !ok {
		tx.Rollback()
		return 0, fmt.Errorf("failed to substitute context in data")
	}
	substitutedWhere, ok := substituteContextValues(where, flatCtx).(map[string]interface{})
	if !ok {
		tx.Rollback()
		return 0, fmt.Errorf("failed to substitute context in where")
	}
	var setParts []string
	var args []interface{}
	for k, v := range substitutedData {
		setParts = append(setParts, fmt.Sprintf("%s = ?", k))
		args = append(args, v)
	}
	query := fmt.Sprintf("UPDATE %s SET %s", table, strings.Join(setParts, ", "))
	whereClause, whereArgs, err := easyrest.BuildWhereClause(substitutedWhere)
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
	var flatCtx map[string]string
	if ctx != nil {
		flatCtx, err = easyrest.FormatToContext(ctx)
		if err != nil {
			tx.Rollback()
			return 0, err
		}
	}
	substitutedWhere, ok := substituteContextValues(where, flatCtx).(map[string]interface{})
	if !ok {
		tx.Rollback()
		return 0, fmt.Errorf("failed to substitute context in where")
	}
	whereClause, args, err := easyrest.BuildWhereClause(substitutedWhere)
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
// It orders the parameters by key to ensure a deterministic order, substitutes context values,
// and executes the function call inside a transaction with context variables applied.
func (p *pgPlugin) CallFunction(userID, funcName string, data map[string]interface{}, ctx map[string]interface{}) (interface{}, error) {
	// Sort parameter keys.
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var placeholders []string
	var args []interface{}
	var flatCtx map[string]string
	var err error
	if ctx != nil {
		flatCtx, err = easyrest.FormatToContext(ctx)
		if err != nil {
			return nil, err
		}
	}
	// Process each parameter.
	for _, k := range keys {
		v := data[k]
		if s, ok := v.(string); ok && strings.HasPrefix(s, "erctx.") {
			key := strings.ToLower(strings.TrimPrefix(s, "erctx."))
			if flatCtx != nil {
				if val, exists := flatCtx[key]; exists {
					placeholders = append(placeholders, "?")
					args = append(args, val)
					continue
				}
			}
		}
		placeholders = append(placeholders, "?")
		args = append(args, v)
	}
	// Build function call query.
	query := fmt.Sprintf("SELECT %s(%s) AS result", funcName, strings.Join(placeholders, ", "))
	query = convertPlaceholders(query)

	// Begin transaction and apply context variables.
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
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	if rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			tx.Rollback()
			return nil, err
		}
		if len(columns) > 0 {
			tx.Commit()
			return columns[0], nil
		}
	}
	tx.Commit()
	return nil, nil
}

// substituteContextValues recursively replaces strings starting with "erctx." using flatCtx.
func substituteContextValues(input interface{}, flatCtx map[string]string) interface{} {
	switch v := input.(type) {
	case string:
		if strings.HasPrefix(v, "erctx.") {
			key := strings.TrimPrefix(v, "erctx.")
			normalizedKey := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
			if val, exists := flatCtx[normalizedKey]; exists {
				return val
			}
			return v
		}
		return v
	case map[string]interface{}:
		m := make(map[string]interface{})
		for key, value := range v {
			m[key] = substituteContextValues(value, flatCtx)
		}
		return m
	case []interface{}:
		s := make([]interface{}, len(v))
		for i, item := range v {
			s[i] = substituteContextValues(item, flatCtx)
		}
		return s
	default:
		return v
	}
}

// convertPlaceholders replaces each "?" in the query with PostgreSQL-style "$1", "$2", ...
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

func main() {
	showVersion := flag.Bool("version", false, "Show version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(Version)
		return
	}
	impl := &pgPlugin{}
	// Serve the plugin using the go-plugin framework.
	hplugin.Serve(&hplugin.ServeConfig{
		HandshakeConfig: easyrest.Handshake,
		Plugins: map[string]hplugin.Plugin{
			"db": &easyrest.DBPluginPlugin{Impl: impl},
		},
	})
}
