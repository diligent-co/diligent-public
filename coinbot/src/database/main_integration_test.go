//go:build integration

package database

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"coinbot/src/config"
)

func TestMainIntegration(t *testing.T) {
	// test reading config and building db connection
	config, err := config.Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}
	db, err := NewDBConnection(config.DatabaseConfig)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// test inserting and reading data
	assert.NotNil(t, db)
}
