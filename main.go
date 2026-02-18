package main

import (
	"log"
	"github.com/yesetoda/query_builder/query_builder"
)

func main() {
	allowedSchema := map[string]map[string]bool{
		"users": {
			"id":         true,
			"name":       true,
			"created_at": true,
		},
		"orders": {
			"id":    true,
			"order": true,
			"time":  true,
			"price": true,
		},
		"customers": {
			"id":      true,
			"user_id": true,
			"name":    true,
			"age":     true,
		},
	}

	ora := query_builder.New(query_builder.OracleDialect{}).
		From("users", "u").WithSchema(allowedSchema).Select("u.name", "u.id", "o.price", "o.time", "c.name", "c.age").Join("INNER", "orders", "o", "u.id", "o.id", "=").
		Join("INNER", "customers", "c", "u.id", "c.user_id", "=").
		Where(query_builder.And(
			query_builder.F("u.id", ">", 18),
			query_builder.F("u.id", "LIKE", "Admin%"),
			query_builder.And(
				query_builder.F("o.price", ">", 100),
				query_builder.F("c.age", "<", 30),
				query_builder.Or(
					query_builder.F("c.name", "LIKE", "%John%"),
					query_builder.F("c.name", "LIKE", "%Jane%"),
				),
			),
		),
	)

	ora.OrderBy("c.name", "ASC")
	ora.OrderBy("u.created_at", "DESC")
	ora.Limit(5)
	ora.Offset(10)
	// ora.Count()

	sql, args, err := ora.Build()
	if err != nil {
		log.Println(err)
	} else {
		log.Println("--- Oracle ---")
		log.Println(sql)
		log.Println("Args:", args)

	}

}
