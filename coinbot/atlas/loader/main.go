package main

import (
	"fmt"
	"io"
	"os"

	_ "ariga.io/atlas-go-sdk/recordriver" // import used by the CLI tool
	"ariga.io/atlas-provider-gorm/gormschema"

	"coinbot/src/database"
)

func main() {
	statements, err := gormschema.New("postgres").Load(database.DbTables...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load GORM schema: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("CREATE EXTENSION IF NOT EXISTS vector;")
	fmt.Println(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`)
	fmt.Println(`CREATE EXTENSION IF NOT EXISTS lo;`)

	io.WriteString(os.Stdout, statements) //nolint:errcheck
}
