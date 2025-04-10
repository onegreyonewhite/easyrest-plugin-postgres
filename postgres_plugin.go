// postgres_plugin.go
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/goccy/go-json"
	hplugin "github.com/hashicorp/go-plugin"
	_ "github.com/lib/pq"
	easyrest "github.com/onegreyonewhite/easyrest/plugin"
)

var Version = "v0.3.3"

type sqlOpener interface {
	Open(driverName, dataSourceName string) (*sql.DB, error)
}

type defaultSQLOpener struct{}

func (o *defaultSQLOpener) Open(driverName, dataSourceName string) (*sql.DB, error) {
	return sql.Open(driverName, dataSourceName)
}

// Add interface for testing purposes
type sqlDB interface {
	SetMaxOpenConns(n int)
	SetMaxIdleConns(n int)
	SetConnMaxLifetime(d time.Duration)
	SetConnMaxIdleTime(d time.Duration)
	Ping() error
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Close() error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// pgPlugin implements the DBPlugin interface for PostgreSQL.
type pgPlugin struct {
	db             sqlDB
	opener         sqlOpener
	defaultTimeout time.Duration // Default timeout for operations
	bulkThreshold  int           // Threshold for bulk operations
}

// newContextWithTimeout creates a new context with timeout
func (p *pgPlugin) newContextWithTimeout() (context.Context, context.CancelFunc) {
	if p.defaultTimeout == 0 {
		p.defaultTimeout = 30 * time.Second
	}
	return context.WithTimeout(context.Background(), p.defaultTimeout)
}

// InitConnection opens a PostgreSQL connection using a URI with the prefix "postgres://".
func (p *pgPlugin) InitConnection(uri string) error {
	if p.opener == nil {
		p.opener = &defaultSQLOpener{}
	}

	if !strings.HasPrefix(uri, "postgres://") {
		return errors.New("invalid postgres URI")
	}

	parsedURI, err := url.Parse(uri)
	if err != nil {
		return err
	}

	queryParams := parsedURI.Query()
	maxOpenConns := 100
	maxIdleConns := 25
	connMaxLifetime := 5
	connMaxIdleTime := 10
	timeout := 30        // Timeout in seconds
	bulkThreshold := 100 // Threshold for bulk operations

	// Remove connection parameters from query before creating DSN
	if val := queryParams.Get("maxOpenConns"); val != "" {
		if n, err := fmt.Sscanf(val, "%d", &maxOpenConns); err != nil || n != 1 {
			return fmt.Errorf("invalid maxOpenConns value: %s", val)
		}
	}
	queryParams.Del("maxOpenConns")

	if val := queryParams.Get("maxIdleConns"); val != "" {
		if n, err := fmt.Sscanf(val, "%d", &maxIdleConns); err != nil || n != 1 {
			return fmt.Errorf("invalid maxIdleConns value: %s", val)
		}
	}
	queryParams.Del("maxIdleConns")

	if val := queryParams.Get("connMaxLifetime"); val != "" {
		if n, err := fmt.Sscanf(val, "%d", &connMaxLifetime); err != nil || n != 1 {
			return fmt.Errorf("invalid connMaxLifetime value: %s", val)
		}
	}
	queryParams.Del("connMaxLifetime")

	if val := queryParams.Get("connMaxIdleTime"); val != "" {
		if n, err := fmt.Sscanf(val, "%d", &connMaxIdleTime); err != nil || n != 1 {
			return fmt.Errorf("invalid connMaxIdleTime value: %s", val)
		}
	}
	queryParams.Del("connMaxIdleTime")

	if val := queryParams.Get("timeout"); val != "" {
		if n, err := fmt.Sscanf(val, "%d", &timeout); err != nil || n != 1 {
			return fmt.Errorf("invalid timeout value: %s", val)
		}
	}
	queryParams.Del("timeout")

	if val := queryParams.Get("bulkThreshold"); val != "" {
		if n, err := fmt.Sscanf(val, "%d", &bulkThreshold); err != nil || n != 1 {
			return fmt.Errorf("invalid bulkThreshold value: %s", val)
		}
	}
	queryParams.Del("bulkThreshold")
	queryParams.Del("autoCleanup")

	parsedURI.RawQuery = queryParams.Encode()
	dsn := parsedURI.String()

	db, err := p.opener.Open("postgres", dsn)
	if err != nil {
		return err
	}

	if db != nil {
		db.SetMaxOpenConns(maxOpenConns)
		db.SetMaxIdleConns(maxIdleConns)
		db.SetConnMaxLifetime(time.Duration(connMaxLifetime) * time.Minute)
		db.SetConnMaxIdleTime(time.Duration(connMaxIdleTime) * time.Minute)
		p.defaultTimeout = time.Duration(timeout) * time.Second
		p.bulkThreshold = bulkThreshold

		ctx, cancel := p.newContextWithTimeout()
		defer cancel()

		if err := db.PingContext(ctx); err != nil {
			return err
		}
		p.db = db
	}

	return nil
}

// getTxPreference extracts the transaction preference ('commit' or 'rollback') from the context.
// Defaults to 'commit' if not specified. Returns an error for invalid values.
func getTxPreference(ctx map[string]any) (string, error) {
	txPreference := "commit" // Default behavior
	if ctx != nil {
		// Use type assertion with checking for existence
		if preferAny, preferExists := ctx["prefer"]; preferExists {
			// Check if prefer is a map
			if prefer, ok := preferAny.(map[string]any); ok {
				// Check if tx exists within prefer
				if txPrefAny, txPrefExists := prefer["tx"]; txPrefExists {
					// Check if tx is a string
					if txPref, ok := txPrefAny.(string); ok && txPref != "" { // Ensure it's a non-empty string
						// Validate the value
						if txPref == "commit" || txPref == "rollback" {
							txPreference = txPref
						} else {
							return "", fmt.Errorf("invalid value for prefer.tx: '%s', must be 'commit' or 'rollback'", txPref)
						}
					} else if !ok {
						// If prefer.tx exists but is not a string
						return "", fmt.Errorf("invalid type for prefer.tx: expected string, got %T", txPrefAny)
					}
					// If txPref is an empty string, we keep the default "commit"
				}
			} else {
				// If prefer exists but is not a map[string]any
				return "", fmt.Errorf("invalid type for prefer: expected map[string]any, got %T", preferAny)
			}
		}
	}
	return txPreference, nil
}

// ApplyPluginContext sets session variables using set_config for all keys in ctx.
// For each key, it sets two variables: one with prefix "request." (complex types are marshaled to JSON)
// and one with prefix "erctx." using the default string representation.
// Keys are processed in sorted order.
func ApplyPluginContext(tx *sql.Tx, ctx map[string]any) error {
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
		case map[string]any, []any:
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

// processRows processes query results in batches
func (p *pgPlugin) processRows(rows *sql.Rows, loc *time.Location) ([]map[string]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	numCols := len(cols)

	// Pre-allocate memory for results
	results := make([]map[string]any, 0, 1000)

	// Reuse slices for scanning
	columns := make([]any, numCols)
	columnPointers := make([]any, numCols)
	for i := range columns {
		columnPointers[i] = &columns[i]
	}

	// Process results in batches
	const batchSize = 1000
	batch := make([]map[string]any, 0, batchSize)

	for rows.Next() {
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		rowMap := make(map[string]any, numCols)
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

		batch = append(batch, rowMap)
		if len(batch) >= batchSize {
			results = append(results, batch...)
			batch = batch[:0] // Clear batch while preserving allocated memory
		}
	}

	// Add remaining results
	if len(batch) > 0 {
		results = append(results, batch...)
	}

	return results, nil
}

// TableGet executes a SELECT query with optional WHERE, GROUP BY, ORDER BY, LIMIT and OFFSET.
func (p *pgPlugin) TableGet(userID, table string, selectFields []string, where map[string]any,
	ordering []string, groupBy []string, limit, offset int, ctx map[string]any) ([]map[string]any, error) {

	queryCtx, cancel := p.newContextWithTimeout()
	defer cancel()

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

	tx, err := p.db.BeginTx(queryCtx, &sql.TxOptions{
		ReadOnly:  true,
		Isolation: sql.LevelReadCommitted,
	})
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

	rows, err := tx.QueryContext(queryCtx, query, args...)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	defer rows.Close()

	results, err := p.processRows(rows, loc)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return results, nil
}

// TableCreate performs an INSERT operation on the specified table
func (p *pgPlugin) TableCreate(userID, table string, data []map[string]any, ctx map[string]any) ([]map[string]any, error) {
	if len(data) == 0 {
		return []map[string]any{}, nil
	}

	// Get transaction preference
	txPreference, err := getTxPreference(ctx)
	if err != nil {
		return nil, err
	}

	// Use COPY for large datasets
	if len(data) >= p.bulkThreshold {
		// Note: bulkCreate currently does not support the rollback preference.
		// It creates a temp table, copies, inserts, and commits within its own transaction.
		if txPreference == "rollback" {
			return nil, fmt.Errorf("rollback preference is not currently supported for bulk create operations (rows >= %d)", p.bulkThreshold)
		}
		return p.bulkCreate(table, data, ctx) // bulkCreate handles its own commit
	}

	tx, err := p.db.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	// Defer rollback in case of errors before the final commit/rollback decision
	defer tx.Rollback() // This will be ignored if Commit() or Rollback() is called explicitly later

	if ctx != nil {
		if err := ApplyPluginContext(tx, ctx); err != nil {
			// tx.Rollback() is handled by defer
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
	var args []any
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
		// tx.Rollback() is handled by defer
		return nil, err
	}

	// Conditionally commit or rollback based on preference
	if txPreference == "rollback" {
		if err := tx.Rollback(); err != nil {
			return nil, fmt.Errorf("failed to rollback transaction: %w", err)
		}
		// Return the data as if it were inserted, even though rolled back
		return data, nil
	} else {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
		return data, nil
	}
}

// bulkCreate uses COPY for efficient insertion of large amounts of data
// TODO: Refactor bulkCreate to accept a transaction and respect txPreference if needed.
func (p *pgPlugin) bulkCreate(table string, data []map[string]any, ctx map[string]any) ([]map[string]any, error) {
	// Note: This function currently manages its own transaction and always commits.
	// The txPreference logic is handled in TableCreate before calling this.
	tx, err := p.db.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	// Get column list
	keys := make([]string, 0, len(data[0]))
	for k := range data[0] {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create temporary table for COPY
	tempTable := fmt.Sprintf("temp_%s_%d", table, time.Now().UnixNano())
	createTempSQL := fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s) ON COMMIT DROP", tempTable, table)
	if _, err := tx.ExecContext(context.Background(), createTempSQL); err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("failed to create temp table: %v", err)
	}

	// Prepare COPY statement
	stmt, err := tx.PrepareContext(context.Background(), fmt.Sprintf("COPY %s (%s) FROM STDIN", tempTable, strings.Join(keys, ", ")))
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("failed to prepare COPY statement: %v", err)
	}
	defer stmt.Close()

	// Copy data
	for _, row := range data {
		values := make([]any, len(keys))
		for i, key := range keys {
			values[i] = row[key]
		}
		if _, err := stmt.ExecContext(context.Background(), values...); err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to copy row: %v", err)
		}
	}

	// Insert data from temporary table into target
	insertSQL := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", table, tempTable)
	if _, err := tx.ExecContext(context.Background(), insertSQL); err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("failed to insert from temp table: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return data, nil
}

// TableUpdate executes an UPDATE query on the specified table.
func (p *pgPlugin) TableUpdate(userID, table string, data map[string]any, where map[string]any, ctx map[string]any) (int, error) {
	// Get transaction preference
	txPreference, err := getTxPreference(ctx)
	if err != nil {
		return 0, err
	}

	tx, err := p.db.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, err
	}
	// Defer rollback in case of errors
	defer tx.Rollback()

	if ctx != nil {
		if err := ApplyPluginContext(tx, ctx); err != nil {
			// tx.Rollback() is handled by defer
			return 0, err
		}
	}
	var setParts []string
	var args []any
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
		// tx.Rollback() is handled by defer
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		// tx.Rollback() is handled by defer
		return 0, err
	}

	// Conditionally commit or rollback
	if txPreference == "rollback" {
		if err := tx.Rollback(); err != nil {
			return 0, fmt.Errorf("failed to rollback transaction: %w", err)
		}
		// Return affected rows count even if rolled back
		return int(affected), nil
	} else {
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("failed to commit transaction: %w", err)
		}
		return int(affected), nil
	}
}

// TableDelete executes a DELETE query on the specified table.
func (p *pgPlugin) TableDelete(userID, table string, where map[string]any, ctx map[string]any) (int, error) {
	// Get transaction preference
	txPreference, err := getTxPreference(ctx)
	if err != nil {
		return 0, err
	}

	tx, err := p.db.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, err
	}
	// Defer rollback in case of errors
	defer tx.Rollback()

	if ctx != nil {
		if err := ApplyPluginContext(tx, ctx); err != nil {
			// tx.Rollback() is handled by defer
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
		// tx.Rollback() is handled by defer
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		// tx.Rollback() is handled by defer
		return 0, err
	}

	// Conditionally commit or rollback
	if txPreference == "rollback" {
		if err := tx.Rollback(); err != nil {
			return 0, fmt.Errorf("failed to rollback transaction: %w", err)
		}
		// Return affected rows count even if rolled back
		return int(affected), nil
	} else {
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("failed to commit transaction: %w", err)
		}
		return int(affected), nil
	}
}

// CallFunction executes a function call using SELECT syntax.
// Parameters are passed in sorted order by key.
// It handles scalar, SETOF, JSON, and void return types.
func (p *pgPlugin) CallFunction(userID, funcName string, data map[string]any, ctx map[string]any) (any, error) {
	queryCtx, cancel := p.newContextWithTimeout()
	defer cancel()

	// Get transaction preference
	txPreference, err := getTxPreference(ctx)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var placeholders []string
	var args []any
	for _, k := range keys {
		placeholders = append(placeholders, "?")
		args = append(args, data[k])
	}

	// Use function name directly, assuming it's safe or properly quoted if needed.
	// The SELECT syntax automatically handles potential multiple return values (SETOF).
	query := fmt.Sprintf("SELECT * FROM %s(%s)", funcName, strings.Join(placeholders, ", "))
	query = convertPlaceholders(query)

	tx, err := p.db.BeginTx(queryCtx, &sql.TxOptions{
		// Although function calls might modify data,
		// we often use ReadCommitted for functions primarily reading data
		// or when the function itself manages transactionality.
		// Consider making this configurable if needed.
		ReadOnly:  false, // Functions can modify data
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Rollback if commit fails or not reached

	loc := time.UTC
	if ctx != nil {
		if err := ApplyPluginContext(tx, ctx); err != nil {
			return nil, fmt.Errorf("failed to apply plugin context: %w", err)
		}
		if tz, ok := ctx["timezone"].(string); ok && tz != "" {
			if l, err := time.LoadLocation(tz); err == nil {
				loc = l
			} else {
				log.Printf("Warning: Could not load timezone '%s': %v", tz, err)
			}
		}
	}

	rows, err := tx.QueryContext(queryCtx, query, args...)
	if err != nil {
		// Check for specific PostgreSQL error indicating function doesn't return SETOF
		// This might require checking the pgerr code. For now, general error handling.
		// If the error suggests it's not a set-returning function, we could retry with SELECT funcName(...)
		return nil, fmt.Errorf("failed to execute function query: %w", err)
	}
	defer rows.Close() // Ensure rows are closed

	// Use processRows to handle potential SETOF results or single composite results
	results, err := p.processRows(rows, loc)
	if err != nil {
		return nil, fmt.Errorf("failed to process function results: %w", err)
	}

	// Check if rows.Err() occurred during iteration in processRows
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during rows iteration: %w", err)
	}

	// Conditionally commit or rollback based on preference
	var finalErr error
	if txPreference == "rollback" {
		if err := tx.Rollback(); err != nil {
			finalErr = fmt.Errorf("failed to rollback transaction: %w", err)
		}
		// No need to log here, behavior is intentional
	} else {
		if err := tx.Commit(); err != nil {
			finalErr = fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	// Return results even if rollback was requested, but prioritize transaction error
	if finalErr != nil {
		return nil, finalErr
	}

	// Analyze the results
	if len(results) == 0 {
		// Function returned void or an empty set
		return nil, nil
	}

	if len(results) == 1 {
		rowMap := results[0]
		if len(rowMap) == 1 {
			// Single row, single column: Potential scalar or JSON
			var value any
			// Get the single value from the map
			for _, v := range rowMap {
				value = v
				break
			}

			switch v := value.(type) {
			case []byte:
				// Attempt to unmarshal if it looks like JSON
				var parsedResult any
				if json.Unmarshal(v, &parsedResult) == nil {
					log.Printf("Successfully unmarshaled []byte result from function %s", funcName)
					return parsedResult, nil
				}
				log.Printf("Result from function %s is []byte but not valid JSON, returning raw bytes", funcName)
				return v, nil // Return raw bytes if not JSON
			case string:
				// Attempt to unmarshal if it looks like JSON
				var parsedResult any
				if json.Unmarshal([]byte(v), &parsedResult) == nil {
					log.Printf("Successfully unmarshaled string result from function %s", funcName)
					return parsedResult, nil
				}
				log.Printf("Result from function %s is string but not valid JSON, returning raw string", funcName)
				return v, nil // Return raw string if not JSON
			default:
				// Return other scalar types directly
				log.Printf("Returning scalar result of type %T from function %s", v, funcName)
				return v, nil
			}
		}
		// Single row, multiple columns: Return as is (map within a slice)
		log.Printf("Returning single composite row result from function %s", funcName)
		return results, nil
	}

	// Multiple rows: Return the slice of maps directly (SETOF result)
	log.Printf("Returning SETOF result (%d rows) from function %s", len(results), funcName)
	return results, nil
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
func (p *pgPlugin) getTablesSchema(tx *sql.Tx) (map[string]any, error) {
	result := make(map[string]any)
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
		properties := make(map[string]any)
		var required []string
		for colRows.Next() {
			var colName, dataType, isNullable string
			var colDefault sql.NullString
			if err := colRows.Scan(&colName, &dataType, &isNullable, &colDefault); err != nil {
				colRows.Close()
				return nil, err
			}
			swType := mapPostgresType(dataType)
			prop := map[string]any{
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
		schema := map[string]any{
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
func (p *pgPlugin) getViewsSchema(tx *sql.Tx) (map[string]any, error) {
	result := make(map[string]any)
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
		properties := make(map[string]any)
		var required []string
		for colRows.Next() {
			var colName, dataType, isNullable string
			var colDefault sql.NullString
			if err := colRows.Scan(&colName, &dataType, &isNullable, &colDefault); err != nil {
				colRows.Close()
				return nil, err
			}
			swType := mapPostgresType(dataType)
			prop := map[string]any{
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
		schema := map[string]any{
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
func (p *pgPlugin) getRPCSchema(tx *sql.Tx) (map[string]any, error) {
	result := make(map[string]any)
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
		properties := make(map[string]any)
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
			prop := map[string]any{
				"type": swType,
			}
			properties[paramName.String] = prop
			reqFields = append(reqFields, paramName.String)
		}
		paramRows.Close()
		inSchema := map[string]any{
			"type":       "object",
			"properties": properties,
		}
		if len(reqFields) > 0 {
			inSchema["required"] = reqFields
		}
		outSchema := map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		}
		if strings.ToLower(r.returnType) != "void" {
			outSchema["properties"] = map[string]any{
				"result": map[string]any{
					"type": mapPostgresType(r.returnType),
				},
			}
		}
		result[r.routineName] = []any{inSchema, outSchema}
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
func (p *pgPlugin) GetSchema(ctx map[string]any) (any, error) {
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
	return map[string]any{
		"tables": tables,
		"views":  views,
		"rpc":    rpc,
	}, nil
}

// pgCachePlugin implements the CachePlugin interface using PostgreSQL.
type pgCachePlugin struct {
	dbPluginPointer *pgPlugin
}

// InitConnection ensures the cache table exists and starts the cleanup goroutine.
// It relies on the underlying pgPlugin's InitConnection being called first or concurrently
// by the plugin framework to establish the database connection.
func (p *pgCachePlugin) InitConnection(uri string) error {
	parsedURL, err := url.Parse(uri)
	if err != nil {
		return fmt.Errorf("failed to parse URI: %w", err)
	}

	// Get the autoCleanup query parameter
	autoCleanup := parsedURL.Query().Get("autoCleanup")

	// Remove the autoCleanup parameter from the URI
	query := parsedURL.Query()
	query.Del("autoCleanup")
	parsedURL.RawQuery = query.Encode()
	uri = parsedURL.String()

	// Ensure the underlying DB connection is initialized.
	// This might be redundant if the framework guarantees calling InitConnection on the DB plugin first,
	// but it ensures dbPluginPointer.db is available.
	if p.dbPluginPointer.db == nil {
		// Attempt to initialize the main plugin connection if not already done.
		// This assumes the same URI is used for both db and cache plugins.
		err := p.dbPluginPointer.InitConnection(uri)
		if err != nil {
			return fmt.Errorf("failed to initialize underlying db connection for cache: %w", err)
		}
	}

	// Check again after potential initialization.
	if p.dbPluginPointer.db == nil {
		return errors.New("database connection not available for cache plugin")
	}

	// Create cache table if it doesn't exist
	// Use TIMESTAMPTZ for timezone awareness
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS easyrest_cache (
		key TEXT PRIMARY KEY,
		value TEXT,
		expires_at TIMESTAMPTZ
	);`
	// Use a background context as table creation is an initialization step.
	_, err = p.dbPluginPointer.db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create cache table: %w", err)
	}

	// Launch background goroutine for cleanup
	if autoCleanup == "true" || autoCleanup == "1" {
		go p.cleanupExpiredCacheEntries()
	}

	return nil
}

// cleanupExpiredCacheEntries periodically deletes expired cache entries.
func (p *pgCachePlugin) cleanupExpiredCacheEntries() {
	// Check more frequently initially, then back off
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		if p.dbPluginPointer.db == nil {
			// Log error if db connection is lost, but keep trying
			fmt.Fprintf(os.Stderr, "Error cleaning cache: DB connection is nil\n")
			continue // Skip this cycle
		}
		// Use NOW() which corresponds to TIMESTAMPTZ
		// Use background context for cleanup task.
		_, err := p.dbPluginPointer.db.ExecContext(context.Background(), "DELETE FROM easyrest_cache WHERE expires_at <= NOW()")
		if err != nil {
			// Log the error, but continue running the cleanup
			fmt.Fprintf(os.Stderr, "Error cleaning up expired cache entries: %v\n", err)
		}
	}
}

// Set stores a key-value pair with a TTL in the cache.
func (p *pgCachePlugin) Set(key string, value string, ttl time.Duration) error {
	if p.dbPluginPointer.db == nil {
		return errors.New("database connection not available for cache set")
	}
	// Calculate expiration time using TIMESTAMPTZ
	expiresAt := time.Now().Add(ttl)
	query := `
	INSERT INTO easyrest_cache (key, value, expires_at)
	VALUES ($1, $2, $3)
	ON CONFLICT (key) DO UPDATE SET
		value = EXCLUDED.value,
		expires_at = EXCLUDED.expires_at;`

	// Use background context for simple cache operation.
	_, err := p.dbPluginPointer.db.ExecContext(context.Background(), query, key, value, expiresAt)
	if err != nil {
		return fmt.Errorf("failed to set cache entry: %w", err)
	}
	return nil
}

// Get retrieves a value from the cache if it exists and hasn't expired.
func (p *pgCachePlugin) Get(key string) (string, error) {
	if p.dbPluginPointer.db == nil {
		return "", errors.New("database connection not available for cache get")
	}
	var value string
	query := "SELECT value FROM easyrest_cache WHERE key = $1 AND expires_at > NOW()"

	// Use background context for simple cache operation.
	err := p.dbPluginPointer.db.QueryRowContext(context.Background(), query, key).Scan(&value)
	if err != nil {
		// Return standard sql.ErrNoRows if not found or expired, otherwise the specific error
		if errors.Is(err, sql.ErrNoRows) {
			return "", sql.ErrNoRows // Standard way to signal cache miss
		}
		return "", fmt.Errorf("failed to get cache entry: %w", err)
	}
	return value, nil
}

func main() {
	showVersion := flag.Bool("version", false, "Show version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(Version)
		return
	}

	// Create a single instance of the core plugin
	impl := &pgPlugin{}
	// Create the cache plugin instance, pointing to the core plugin instance
	cacheImpl := &pgCachePlugin{dbPluginPointer: impl}

	hplugin.Serve(&hplugin.ServeConfig{
		HandshakeConfig: easyrest.Handshake,
		Plugins: map[string]hplugin.Plugin{
			"db":    &easyrest.DBPluginPlugin{Impl: impl},
			"cache": &easyrest.CachePluginPlugin{Impl: cacheImpl}, // Register cache plugin
		},
	})
}
