package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/linkedin/goavro/v2"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type GatewayMetadataAndPayload struct {
	Header  map[string]string `json:"header"`
	Payload []byte            `json:"payload"`
}

func main() {
	// Path to your Parquet file
	parquetFilePath := "~/Downloads/1736944943-b65ee658-19f3-4055-a21f-bcb8bf74fe4b-"

	// Avro schema for deserializing the payload
	avroSchema := `
	{"type" : "record",
	"name" : "GatewayMetadataAndPayload",
	"namespace" : "org.transformedPayload",
	"fields" : [ {
		"name" : "header",
		"type" : {
		"type" : "map",
		"values" : "string"
		}
	}, {
		"name" : "payload",
		"type" : "bytes"
	}]}`

	// Open the Parquet file
	fr, err := local.NewLocalFileReader(parquetFilePath)
	if err != nil {
		log.Fatalf("Failed to open Parquet file: %v", err)
	}
	defer fr.Close()

	// Create a Parquet reader
	pr, err := reader.NewParquetReader(fr, new(GatewayMetadataAndPayload), 4)
	if err != nil {
		log.Fatalf("Failed to create Parquet reader: %v", err)
	}
	defer pr.ReadStop()

	// Prepare the Avro codec for deserializing the payload
	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		log.Fatalf("Failed to create Avro codec: %v", err)
	}

	// Read all rows from the Parquet file
	num := int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		data := make([]GatewayMetadataAndPayload, 1)
		if err := pr.Read(&data); err != nil {
			log.Fatalf("Failed to read row %d: %v", i, err)
		}

		// Decode the payload field
		payload := data[0].Payload
		decoded, err := codec.Decode(bytes.NewReader(payload))
		if err != nil {
			log.Printf("Failed to decode payload for row %d: %v", i, err)
			continue
		}

		// Print the row information
		fmt.Printf("Row %d:\n", i+1)
		fmt.Printf("Header: %v\n", data[0].Header)
		fmt.Printf("Payload: %s\n", prettyPrintJSON(decoded))
	}
}

// prettyPrintJSON converts an interface{} to a pretty JSON string
func prettyPrintJSON(data interface{}) string {
	prettyJSON, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal JSON: %v", err)
		return "{}"
	}
	return string(prettyJSON)
}
