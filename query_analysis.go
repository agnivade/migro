package main

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v4"
)

func analyzeQuery(query string) error {
	tree, err := pg_query.Parse(query)
	if err != nil {
		return err
	}
	fmt.Println("QUERY ANALYSIS:")

	for _, stmt := range tree.Stmts {
		switch typedNode := stmt.Stmt.GetNode().(type) {
		// TODO: Later add more suggestions for these cases.
		// case *pg_query.Node_AlterTableStmt:
		// case *pg_query.Node_UpdateStmt:
		// case *pg_query.Node_DropStmt:
		case *pg_query.Node_IndexStmt:
			if !typedNode.IndexStmt.Concurrent {
				fmt.Println("CREATE INDEX statement does not use concurrent index addition. Recommend adding the clause \"CONCURRENT\"")
			}
		}
	}

	return nil
}
