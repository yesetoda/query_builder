// Package query_builder builds parameterized SQL queries using a fluent API.
//
// The package focuses on two goals:
//
//   - Composability: chainable methods for SELECT, JOIN, WHERE, ORDER BY, and pagination.
//   - Safety: value parameterization and optional table/column allow-list validation.
//
// A Query is created with New and a Dialect implementation.
//
// Basic usage:
//
//	qb := query_builder.New(query_builder.PostgresDialect{}).
//		From("users", "u").
//		Select("u.id", "u.name").
//		Where(query_builder.And(
//			query_builder.F("u.id", ">", 10),
//			query_builder.F("u.name", "LIKE", "A%"),
//		)).
//		OrderBy("u.id", "DESC").
//		Limit(25)
//
//	sql, args, err := qb.Build()
//
// Schema validation is optional. When configured through WithSchema, every table
// and column reference must exist in the provided map, which helps catch mistakes
// early and prevents untrusted identifiers from being used.
package query_builder
