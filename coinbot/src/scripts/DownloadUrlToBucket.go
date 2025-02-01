package main

import (
	"context"
	"log"
	"os"

	"cloud.google.com/go/storage"
	_ "github.com/lib/pq" // or whatever database driver you're using

	"coinbot/src/config"
	"coinbot/src/utils/general"
)

// Event represents your data structure
type Event struct {
	Timestamp string
	EventType string
	UserID    string
	// Add other fields as needed
}

func main() {
	// get url from command line args
	url := os.Args[1]
	if url == "" {
		log.Fatalf("No URL provided")
	}
	// check if url is valid
	valid, err := general.IsValidURL(url)
	if !valid {
		log.Fatalf("Invalid URL: %v", err)
	}

	ctx := context.Background()
	_, configErr := config.Load()
	if configErr != nil {
		log.Fatalf("Failed to load config: %v", configErr)
	}

	// storageConfig := config.StorageConfig
	// bucketName := storageConfig.Bucket
	// prefix := storageConfig.KrakenPrefix

	// Initialize GCS client
	client, storageClientErr := storage.NewClient(ctx)
	if storageClientErr != nil {
		log.Fatalf("Failed to create client: %v", storageClientErr)
	}
	defer client.Close()

	// // Open the bucket and file
	// bucket := client.Bucket(bucketName)
	// obj := bucket.Object(prefix)
	// reader, err := obj.NewReader(ctx)
	// if err != nil {
	// 	log.Fatalf("Failed to create reader: %v", err)
	// }
	// defer reader.Close()

	// // Initialize CSV reader
	// csvReader := csv.NewReader(reader)
	// // Skip header if needed
	// _, err = csvReader.Read()
	// if err != nil {
	// 	log.Fatalf("Failed to read header: %v", err)
	// }

	// // spin off a goroutine to conduct the download, and report status

	// err = general.CopyURLToBucket(ctx, url, bucketName, prefix)
	// if err != nil {
	// 	log.Fatalf("Failed to copy URL to bucket: %v", err)
	// }

}
