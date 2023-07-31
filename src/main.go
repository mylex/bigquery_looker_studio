package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

func main() {
	// Replace the following variables with your own values
	projectID := "your-project-id"
	datasetID := "your-dataset-id"
	tableID := "your-table-id"
	csvFilePath := "path/to/your/csvfile.csv" // e.g., "/home/user/data.csv"

	ctx := context.Background()

	// Set up the BigQuery client
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile("path/to/your/service-account-key.json"))
	if err != nil {
		log.Fatalf("Error creating BigQuery client: %v", err)
	}
	defer client.Close()

	// Create the dataset if it doesn't exist
	dataset := client.Dataset(datasetID)
	if err := dataset.Create(ctx, nil); err != nil {
		if !bigquery.ErrBucketAlreadyExists(err) {
			log.Fatalf("Error creating dataset: %v", err)
		}
	}

	// Create the table schema based on your CSV data structure
	schema, err := inferSchemaFromCSV(csvFilePath)
	if err != nil {
		log.Fatalf("Error inferring schema from CSV: %v", err)
	}

	// Create the table if it doesn't exist
	table := dataset.Table(tableID)
	if err := table.Create(ctx, &bigquery.TableMetadata{
		Schema: schema,
	}); err != nil {
		if !bigquery.ErrBucketAlreadyExists(err) {
			log.Fatalf("Error creating table: %v", err)
		}
	}

	// Open the CSV file
	file, err := os.Open(csvFilePath)
	if err != nil {
		log.Fatalf("Error opening CSV file: %v", err)
	}
	defer file.Close()

	// Create the BigQuery loader
	loader := table.LoaderFrom(file)
	loader.SourceFormat = bigquery.CSV

	// Load the data into BigQuery
	job, err := loader.Run(ctx)
	if err != nil {
		log.Fatalf("Error loading data into BigQuery: %v", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		log.Fatalf("Error waiting for job completion: %v", err)
	}

	if err := status.Err(); err != nil {
		log.Fatalf("Error loading data into BigQuery: %v", err)
	}

	fmt.Println("Data imported successfully!")
}

// inferSchemaFromCSV infers the BigQuery schema from a CSV file
func inferSchemaFromCSV(csvFilePath string) (bigquery.Schema, error) {
	file, err := os.Open(csvFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read the first line to get the header row
	headerRow, err := bigquery.NewReader(file).Read(1)
	if err != nil {
		return nil, err
	}

	// Create the schema based on the header row
	schema := make(bigquery.Schema, len(headerRow))
	for i, columnName := range headerRow {
		schema[i] = &bigquery.FieldSchema{
			Name: columnName,
			Type: bigquery.StringFieldType,
		}
	}

	return schema, nil
}
