package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

// TestImportCSVtoBigQuery tests the functionality of importing a CSV file to a BigQuery table.
func TestImportCSVtoBigQuery(t *testing.T) {
	// Replace the following variables with your own values
	projectID := "your-project-id"
	datasetID := "your-dataset-id"
	tableID := "your-table-id"
	csvFilePath := "path/to/your/test-csvfile.csv" // e.g., "testdata/testdata.csv"

	ctx := context.Background()

	// Set up the BigQuery client
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile("path/to/your/service-account-key.json"))
	if err != nil {
		t.Fatalf("Error creating BigQuery client: %v", err)
	}
	defer client.Close()

	// Create a dataset for testing purposes
	dataset := client.Dataset(datasetID + "_test")
	if err := dataset.Create(ctx, nil); err != nil {
		t.Fatalf("Error creating dataset for testing: %v", err)
	}
	defer dataset.DeleteWithContents(ctx)

	// Create a test table with the same schema as the actual table
	testTableID := tableID + "_test"
	table := dataset.Table(testTableID)
	if err := table.Create(ctx, &bigquery.TableMetadata{
		Schema: inferSchemaFromCSV(csvFilePath),
	}); err != nil {
		t.Fatalf("Error creating test table: %v", err)
	}
	defer table.Delete(ctx)

	// Open the test CSV file
	file, err := os.Open(csvFilePath)
	if err != nil {
		t.Fatalf("Error opening test CSV file: %v", err)
	}
	defer file.Close()

	// Create the BigQuery loader
	loader := table.LoaderFrom(file)
	loader.SourceFormat = bigquery.CSV

	// Load the test data into BigQuery
	job, err := loader.Run(ctx)
	if err != nil {
		t.Fatalf("Error loading test data into BigQuery: %v", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatalf("Error waiting for test job completion: %v", err)
	}

	if err := status.Err(); err != nil {
		t.Fatalf("Error loading test data into BigQuery: %v", err)
	}

	fmt.Println("Test data imported successfully!")
}

// You can add more test cases or edge cases as needed.

// Test function to infer schema from CSV (dummy test)
func TestInferSchemaFromCSV(t *testing.T) {
	csvFilePath := "path/to/your/test-csvfile.csv" // e.g., "testdata/testdata.csv"
	schema, err := inferSchemaFromCSV(csvFilePath)
	if err != nil {
		t.Fatalf("Error inferring schema from CSV: %v", err)
	}

	// Test your schema here if necessary.
}

// TestMain is a test entry point to run all tests.
func TestMain(m *testing.M) {
	// Run the tests
	exitCode := m.Run()

	// Add any additional cleanup or teardown logic here if necessary.

	// Exit with the test result
	os.Exit(exitCode)
}
