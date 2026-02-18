package query_builder

import (
	"errors"
	"fmt"
	"strings"
)

// maxFilterDepth prevents extremely nested filters that could cause stack overflow or performance issues.
const maxFilterDepth = 10

// Dialect defines SQL flavor-specific behavior like placeholder syntax and identifier quoting.
// Users can implement this interface to support additional database systems.
type Dialect interface {
	// Placeholder returns the positional or non-positional parameter placeholder for the given index (1-based).
	// For Postgres, this might return "$1"; for MySQL, "?"; for Oracle, ":1".
	Placeholder(index int) string
	// QuoteIdentifier wraps a table or column name with the appropriate quotes for the dialect.
	// For Postgres/Oracle, this uses double quotes (""); for MySQL, it uses backticks (``).
	QuoteIdentifier(name string) string
}

// PostgresDialect implements Dialect for PostgreSQL, using $1, $2 placeholders and double quotes.
type PostgresDialect struct{}

// Placeholder returns $1, $2, etc.
func (p PostgresDialect) Placeholder(index int) string {
	return fmt.Sprintf("$%d", index)
}

// QuoteIdentifier returns "name".
func (p PostgresDialect) QuoteIdentifier(name string) string {
	return fmt.Sprintf("\"%s\"", name)
}

// MySQLDialect implements Dialect for MySQL, using ? placeholders and backticks.
type MySQLDialect struct{}

// Placeholder returns ?.
func (m MySQLDialect) Placeholder(index int) string {
	return "?"
}

// QuoteIdentifier returns `name`.
func (m MySQLDialect) QuoteIdentifier(name string) string {
	return fmt.Sprintf("`%s`", name)
}

// OracleDialect implements Dialect for Oracle, using :1, :2 placeholders and modern pagination.
type OracleDialect struct{}

// Placeholder returns :1, :2, etc.
func (o OracleDialect) Placeholder(index int) string {
	return fmt.Sprintf(":%d", index)
}

// QuoteIdentifier returns "name".
func (o OracleDialect) QuoteIdentifier(name string) string {
	return fmt.Sprintf("\"%s\"", name)
}

// Query builds a SQL SELECT or COUNT statement.
//
// A Query is configured through chainable methods and rendered with Build.
// When WithSchema is set, table and column references are validated.
type Query struct {
	dialect       Dialect                    // The target SQL dialect (Postgres, MySQL, Oracle)
	allowedSchema map[string]map[string]bool // Validation schema: map[table]map[column]bool
	baseTable     string                     // The main table to select from
	baseAlias     string                     // Alias for the base table
	projections   []ColumnRef                // List of columns to SELECT
	joins         []Join                     // List of JOIN clauses
	where         *FilterGroup               // Root filter group (WHERE clause)
	sorts         []Sort                     // List of columns to ORDER BY
	limit         int                        // Maximum rows to fetch
	offset        int                        // Rows to skip (if using Offset pagination)
	pagination    Pagination                 // Detailed pagination configuration
	isCount       bool                       // If true, generates SELECT COUNT(*)
	errors        []error                    // Collection of errors encountered during building
}

// ColumnRef represents a reference to a table column, optionally with a table alias.
type ColumnRef struct {
	TableAlias string // The alias of the table (e.g., "u" in "u.name")
	ColumnName string // The name of the column (e.g., "name" in "u.name")
}

// Col is a helper that parses a string into a ColumnRef.
// If the string contains a dot (e.g., "u.id"), it splits it into alias and name.
// Otherwise, it assumes it's just a column name.
func Col(ref string) ColumnRef {
	parts := strings.Split(ref, ".")
	if len(parts) == 2 {
		return ColumnRef{TableAlias: parts[0], ColumnName: parts[1]}
	}
	return ColumnRef{ColumnName: ref}
}

// Join represents a SQL JOIN clause, including the type, target table, and its alias.
type Join struct {
	Type      string        // e.g., "INNER", "LEFT"
	Table     string        // Target table name
	Alias     string        // Alias for the target table
	Condition JoinCondition // The ON clause condition
}

// JoinCondition represents the comparison in a JOIN ... ON clause.
type JoinCondition struct {
	Left  ColumnRef // Left side of the expression
	Op    string    // Comparison operator (e.g., "=")
	Right ColumnRef // Right side of the expression
}

// FilterGroup combines filters and nested groups with a logical operator.
type FilterGroup struct {
	Operator string        // "AND" or "OR"
	Filters  []Filter      // Individual comparisons
	Groups   []FilterGroup // Nested groups
}

// And returns a FilterGroup that joins its items with AND.
//
// Items may be either Filter values or *FilterGroup values.
func And(filters ...interface{}) *FilterGroup {
	return createGroup("AND", filters...)
}

// Or returns a FilterGroup that joins its items with OR.
//
// Items may be either Filter values or *FilterGroup values.
func Or(filters ...interface{}) *FilterGroup {
	return createGroup("OR", filters...)
}

// createGroup builds a FilterGroup from Filter and *FilterGroup items.
func createGroup(op string, items ...interface{}) *FilterGroup {
	g := &FilterGroup{Operator: op}
	for _, item := range items {
		switch v := item.(type) {
		case Filter:
			g.Filters = append(g.Filters, v)
		case *FilterGroup:
			if v != nil {
				g.Groups = append(g.Groups, *v)
			}
		}
	}
	return g
}

// Filter represents a single comparison in a WHERE clause (e.g., "age > 18").
type Filter struct {
	Column ColumnRef   // The column to filter on
	Op     string      // The operator (e.g., "=", ">", "LIKE", "IN")
	Value  interface{} // The value to compare against (will be parameterized)
}

// F constructs a single Filter.
//
// ref may be either "alias.column" or just "column".
func F(ref string, op string, val interface{}) Filter {
	return Filter{Column: Col(ref), Op: op, Value: val}
}

// Sort represents a single column ordering in the ORDER BY clause.
type Sort struct {
	Column ColumnRef // The column to sort by
	Dir    string    // Sort direction: "ASC" or "DESC"
}

// Pagination configures how results should be limited and paged.
type Pagination struct {
	Type     string                 // "offset" (standard) or "keyset" (cursor-based)
	LastSeen map[string]interface{} // Values of sorting columns from the last page (for Keyset)
}

// Internal allow-lists for operators, join types, and sort directions.
var allowedOperators = map[string]bool{
	"=": true, "!=": true, ">": true, "<": true, ">=": true, "<=": true, "IN": true, "LIKE": true, "IS": true, "IS NOT": true,
}

var allowedJoinTypes = map[string]bool{
	"INNER": true, "LEFT": true, "RIGHT": true, "FULL": true, "CROSS": true,
}

var allowedSortDir = map[string]bool{
	"ASC": true, "DESC": true,
}

// New returns a Query that uses the provided SQL dialect.
func New(dialect Dialect) *Query {
	return &Query{
		dialect: dialect,
	}
}

// WithSchema sets an optional validation schema.
//
// When schema is provided, Build returns an error for unknown tables or columns.
// The expected format is map[tableName][columnName]bool.
func (q *Query) WithSchema(schema map[string]map[string]bool) *Query {
	q.allowedSchema = schema
	return q
}

// From sets the primary table and its alias for the query.
// Example: From("users", "u")
func (q *Query) From(table string, alias string) *Query {
	q.baseTable = table
	q.baseAlias = alias
	return q
}

// Select adds one or more projection columns.
//
// Each entry is usually "alias.column".
func (q *Query) Select(columns ...string) *Query {
	for _, col := range columns {
		q.projections = append(q.projections, Col(col))
	}
	return q
}

// Count switches output to SELECT COUNT(*) for the configured query.
//
// Projection columns are ignored in count mode.
func (q *Query) Count() *Query {
	q.isCount = true
	return q
}

// Join adds a JOIN clause.
//
// joinType must be one of INNER, LEFT, RIGHT, FULL, or CROSS.
// left and right are column references used in the ON condition.
func (q *Query) Join(joinType, table, alias, left, right, op string) *Query {
	q.joins = append(q.joins, Join{
		Type:  joinType,
		Table: table,
		Alias: alias,
		Condition: JoinCondition{
			Left:  Col(left),
			Op:    op,
			Right: Col(right),
		},
	})
	return q
}

// Where sets the root WHERE filter group.
//
// Use And and Or to compose nested conditions.
func (q *Query) Where(group *FilterGroup) *Query {
	q.where = group
	return q
}

// Eq appends an equality filter to the current WHERE group.
//
// If no WHERE group exists, Eq creates an AND group first.
func (q *Query) Eq(ref string, val interface{}) *Query {
	filter := F(ref, "=", val)
	if q.where == nil {
		q.where = And(filter)
	} else {
		q.where.Filters = append(q.where.Filters, filter)
	}
	return q
}

// In appends an IN filter to the current WHERE group.
//
// val should be a slice value compatible with your SQL driver.
func (q *Query) In(ref string, val interface{}) *Query {
	filter := F(ref, "IN", val)
	if q.where == nil {
		q.where = And(filter)
	} else {
		q.where.Filters = append(q.where.Filters, filter)
	}
	return q
}

// OrderBy appends a sort column and direction.
//
// dir should be ASC or DESC.
func (q *Query) OrderBy(column string, dir string) *Query {
	q.sorts = append(q.sorts, Sort{Column: Col(column), Dir: strings.ToUpper(dir)})
	return q
}

// Limit sets the maximum number of rows to return.
func (q *Query) Limit(limit int) *Query {
	q.limit = limit
	return q
}

// Offset sets the number of rows to skip.
//
// Calling Offset sets pagination mode to "offset".
func (q *Query) Offset(offset int) *Query {
	q.offset = offset
	q.pagination.Type = "offset"
	return q
}

// KeysetPagination configures cursor-based paging.
//
// lastSeen keys must use the "alias.column" form and match sort columns.
func (q *Query) KeysetPagination(lastSeen map[string]interface{}) *Query {
	q.pagination = Pagination{
		Type:     "keyset",
		LastSeen: lastSeen,
	}
	return q
}

// Build renders the SQL statement and bound arguments.
//
// Build validates table and column references when schema validation is enabled.
func (q *Query) Build() (string, []interface{}, error) {
	if len(q.errors) > 0 {
		return "", nil, q.errors[0]
	}
	// Basic sanity check on the base table.
	if err := q.validateBase(q.allowedSchema); err != nil {
		return "", nil, err
	}

	var sb strings.Builder
	var args []interface{}

	// Register all table aliases to ensure visibility during column validation.
	aliasMap, err := q.registerAliases()
	if err != nil {
		return "", nil, err
	}

	// 1. SELECT phase
	if q.isCount {
		sb.WriteString("SELECT COUNT(*)")
	} else {
		if err := q.buildProjections(&sb, aliasMap, q.allowedSchema, q.getBaseAlias()); err != nil {
			return "", nil, err
		}
	}

	// 2. FROM phase
	sb.WriteString(fmt.Sprintf(" FROM %s %s", q.baseTable, q.getBaseAlias()))

	// 3. JOIN phase
	if err := q.buildJoins(&sb, aliasMap, q.allowedSchema); err != nil {
		return "", nil, err
	}

	// 4. WHERE phase (includes standard filters and Keyset pagination filters)
	if err := q.buildFilters(&sb, &args, aliasMap, q.allowedSchema); err != nil {
		return "", nil, err
	}

	// Count queries generally finalize after the WHERE clause.
	// so no need to build the order and also the pagination
	if q.isCount {
		return sb.String(), args, nil
	}

	// 5. ORDER BY phase
	if err := q.buildOrderBy(&sb, aliasMap, q.allowedSchema); err != nil {
		return "", nil, err
	}

	// 6. LIMIT/OFFSET phase (Dialect-specific syntax)
	q.buildLimitOffset(&sb, &args)

	return sb.String(), args, nil
}

// validateBase ensures a primary table is selected and exists in the schema.
func (q *Query) validateBase(schema map[string]map[string]bool) error {
	if q.baseTable == "" {
		return errors.New("base table required")
	}
	if schema != nil {
		if _, ok := schema[q.baseTable]; !ok {
			return fmt.Errorf("invalid base table: %s", q.baseTable)
		}
	}
	return nil
}

// getBaseAlias returns the explicit alias or the table name if no alias exists.
func (q *Query) getBaseAlias() string {
	if q.baseAlias != "" {
		return q.baseAlias
	}
	return q.baseTable
}

// registerAliases creates a mapping of alias -> tableName for validation.
func (q *Query) registerAliases() (map[string]string, error) {
	aliasMap := make(map[string]string)
	aliasMap[q.getBaseAlias()] = q.baseTable

	for _, j := range q.joins {
		if j.Alias == "" {
			return nil, errors.New("join alias required")
		}
		if _, exists := aliasMap[j.Alias]; exists {
			return nil, fmt.Errorf("duplicate alias: %s", j.Alias)
		}
		aliasMap[j.Alias] = j.Table
	}
	return aliasMap, nil
}

// buildFilters translates the filter tree into a SQL WHERE clause.
func (q *Query) buildFilters(sb *strings.Builder, args *[]interface{}, aliasMap map[string]string, schema map[string]map[string]bool) error {
	hasWhere := false
	if q.where != nil {
		whereClause, err := q.buildFilterGroup(*q.where, args, aliasMap, 0, schema)
		if err != nil {
			return err
		}
		if whereClause != "" {
			sb.WriteString(" WHERE ")
			sb.WriteString(whereClause)
			hasWhere = true
		}
	}

	// Append Keyset constraints if applicable.
	if q.pagination.Type == "keyset" && len(q.sorts) > 0 {
		keysetClause, err := q.buildKeysetPagination(args, hasWhere)
		if err != nil {
			return err
		}
		if keysetClause != "" {
			if !hasWhere {
				sb.WriteString(" WHERE ")
			} else {
				sb.WriteString(" AND ")
			}
			sb.WriteString(keysetClause)
		}
	}
	return nil
}

// buildOrderBy generates the ORDER BY clause with validation.
func (q *Query) buildOrderBy(sb *strings.Builder, aliasMap map[string]string, schema map[string]map[string]bool) error {
	if len(q.sorts) == 0 {
		return nil
	}
	sb.WriteString(" ORDER BY ")
	var sortParts []string
	for _, s := range q.sorts {
		if err := q.validateCol(s.Column, aliasMap, schema); err != nil {
			return fmt.Errorf("invalid sort column: %v", err)
		}
		dir := strings.ToUpper(s.Dir)
		if !allowedSortDir[dir] {
			return fmt.Errorf("invalid sort direction: %s", s.Dir)
		}
		sortParts = append(sortParts, fmt.Sprintf("%s.%s %s", s.Column.TableAlias, s.Column.ColumnName, dir))
	}
	sb.WriteString(strings.Join(sortParts, ", "))
	return nil
}

// buildLimitOffset adds pagination clauses using standard or dialect-specific (Oracle) syntax.
func (q *Query) buildLimitOffset(sb *strings.Builder, args *[]interface{}) {
	if q.limit <= 0 {
		return
	}
	// Use FETCH NEXT ... syntax for Oracle or Keyset-based paging.
	if q.pagination.Type == "keyset" || q.dialect.Placeholder(1) == ":1" {
		*args = append(*args, q.limit)
		sb.WriteString(fmt.Sprintf(" FETCH NEXT %s ROWS ONLY", q.dialect.Placeholder(len(*args))))
	} else {
		*args = append(*args, q.limit)
		sb.WriteString(fmt.Sprintf(" LIMIT %s", q.dialect.Placeholder(len(*args))))
		if q.offset > 0 {
			*args = append(*args, q.offset)
			sb.WriteString(fmt.Sprintf(" OFFSET %s", q.dialect.Placeholder(len(*args))))
		}
	}
}

// buildProjections generates the SELECT column list.
func (q *Query) buildProjections(sb *strings.Builder, aliasMap map[string]string, schema map[string]map[string]bool, baseAlias string) error {
	sb.WriteString("SELECT ")
	if len(q.projections) == 0 {
		sb.WriteString(baseAlias + ".*")
	} else {
		var cols []string
		for _, p := range q.projections {
			if err := q.validateCol(p, aliasMap, schema); err != nil {
				return fmt.Errorf("invalid column: %v", err)
			}
			cols = append(cols, fmt.Sprintf("%s.%s", p.TableAlias, p.ColumnName))
		}
		sb.WriteString(strings.Join(cols, ", "))
	}
	return nil
}

// buildJoins iteratively builds all JOIN clauses.
func (q *Query) buildJoins(sb *strings.Builder, aliasMap map[string]string, schema map[string]map[string]bool) error {
	for _, j := range q.joins {
		if err := q.validateJoin(j, aliasMap, schema); err != nil {
			return err
		}
		sb.WriteString(fmt.Sprintf(" %s JOIN %s %s ON %s.%s %s %s.%s",
			strings.ToUpper(j.Type), j.Table, j.Alias,
			j.Condition.Left.TableAlias, j.Condition.Left.ColumnName,
			j.Condition.Op,
			j.Condition.Right.TableAlias, j.Condition.Right.ColumnName,
		))
	}
	return nil
}

// validateJoin checks join types, tables, and columns against settings/schema.
func (q *Query) validateJoin(j Join, aliasMap map[string]string, schema map[string]map[string]bool) error {
	if !allowedJoinTypes[strings.ToUpper(j.Type)] {
		return fmt.Errorf("invalid join type: %s", j.Type)
	}
	if schema == nil {
		return nil
	}
	if _, ok := schema[j.Table]; !ok {
		return fmt.Errorf("invalid join table: %s", j.Table)
	}
	if err := q.validateCol(j.Condition.Left, aliasMap, schema); err != nil {
		return fmt.Errorf("invalid join left column: %v", err)
	}
	if err := q.validateCol(j.Condition.Right, aliasMap, schema); err != nil {
		return fmt.Errorf("invalid join right column: %v", err)
	}
	return nil
}

// validateCol ensures a column reference is valid within its table and the schema.
func (q *Query) validateCol(ref ColumnRef, aliasMap map[string]string, schema map[string]map[string]bool) error {
	if schema == nil {
		return nil
	}
	tableName, ok := aliasMap[ref.TableAlias]
	if !ok || !schema[tableName][ref.ColumnName] {
		return fmt.Errorf("%s.%s", ref.TableAlias, ref.ColumnName)
	}
	return nil
}

// buildFilterGroup recursively builds nested AND/OR groups.
func (q *Query) buildFilterGroup(g FilterGroup, args *[]interface{}, aliasMap map[string]string, depth int, schema map[string]map[string]bool) (string, error) {
	if depth > maxFilterDepth {
		return "", errors.New("filter depth exceeded")
	}
	op := strings.ToUpper(g.Operator)
	if op != "AND" && op != "OR" {
		return "", errors.New("invalid logical operator")
	}

	parts, err := q.collectFilters(g.Filters, args, aliasMap, schema)
	if err != nil {
		return "", err
	}

	for _, subGroup := range g.Groups {
		sub, err := q.buildFilterGroup(subGroup, args, aliasMap, depth+1, schema)
		if err != nil {
			return "", err
		}
		if sub != "" {
			parts = append(parts, "("+sub+")")
		}
	}

	if len(parts) == 0 {
		return "", nil
	}
	return strings.Join(parts, " "+op+" "), nil
}

// collectFilters validates and parameterizes individual filters in a group.
func (q *Query) collectFilters(filters []Filter, args *[]interface{}, aliasMap map[string]string, schema map[string]map[string]bool) ([]string, error) {
	var parts []string
	for _, f := range filters {
		if err := q.validateCol(f.Column, aliasMap, schema); err != nil {
			return nil, fmt.Errorf("invalid column: %v", err)
		}
		if !allowedOperators[strings.ToUpper(f.Op)] {
			return nil, fmt.Errorf("invalid operator: %s", f.Op)
		}

		*args = append(*args, f.Value)
		parts = append(parts, fmt.Sprintf("%s.%s %s %s",
			f.Column.TableAlias,
			f.Column.ColumnName,
			f.Op,
			q.dialect.Placeholder(len(*args)),
		))
	}
	return parts, nil
}

// buildKeysetPagination generates the cursor-based comparison for paging.
func (q *Query) buildKeysetPagination(args *[]interface{}, hasWhere bool) (string, error) {
	if q.pagination.Type != "keyset" || len(q.sorts) == 0 {
		return "", nil
	}

	col := q.sorts[0].Column
	key := col.TableAlias + "." + col.ColumnName
	val, ok := q.pagination.LastSeen[key]
	if !ok {
		return "", nil
	}

	op := ">"
	if strings.ToUpper(q.sorts[0].Dir) == "DESC" {
		op = "<"
	}

	*args = append(*args, val)
	return fmt.Sprintf("%s.%s %s %s", col.TableAlias, col.ColumnName, op, q.dialect.Placeholder(len(*args))), nil
}
