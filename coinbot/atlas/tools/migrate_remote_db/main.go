package main

import (
	"fmt"
	"os"
	"os/exec"

	"coinbot/src/config"
	"coinbot/src/database"
)

// This script run atlas migrations reading db credential from GCP secrets
// Later it should be a post-install helm hook

func main() {
	appConfig, err := config.Load()
	if err != nil {
		fmt.Printf("❌ failed to load config: %v", err)
		os.Exit(1)
	}

	uri := database.MakeConnectionString(&appConfig.DatabaseConfig)

	fmt.Printf("Executing migrations against db at: %s\n", uri)

	cmd := exec.Command("atlas", "migrate", "apply",
		"--url", uri,
		"--dir", "file://atlas/migrations",
	)
	output, err := cmd.CombinedOutput()

	fmt.Print(string(output))

	if err != nil {
		fmt.Printf("❌ failed to run Atlas migrations: %v", err)
		os.Exit(1)
	}
}
