package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const dbDriverName string = "postgres"

var dbDataSourceName = fmt.Sprintf(
	"host=%s dbname=%s user=%s password=%s",
	os.Getenv("DB_HOST"),
	os.Getenv("DB_NAME"),
	os.Getenv("DB_USER"),
	os.Getenv("DB_PASSWORD"),
)

// see https://docs.aws.amazon.com/lambda/latest/dg/with-sns-create-package.html#with-sns-example-deployment-pkg-go
// see https://godoc.org/github.com/aws/aws-lambda-go/events
func getS3EventFromSNSEvent(snsEvent events.SNSEvent) (*events.S3Event, error) {
	var s3Event events.S3Event
	if len(snsEvent.Records) != 1 {
		return nil, errors.New("Unsupported format: more than 1 record in the SNS Event")
	}
	record := snsEvent.Records[0]
	snsRecord := record.SNS
	if err := json.Unmarshal([]byte(snsRecord.Message), &s3Event); err != nil {
		return nil, err
	}
	return &s3Event, nil
}

func parseS3Event(s3Event *events.S3Event) (string, string, error) {
	if len(s3Event.Records) != 1 {
		return "", "", errors.New("Unsupported format: more than 1 record in the S3 Event")
	}
	record := s3Event.Records[0]
	bucket := record.S3.Bucket.Name
	objectKey := record.S3.Object.Key
	return bucket, objectKey, nil
}

func handler(ctx context.Context, snsEvent events.SNSEvent) error {
	// Extract S3 bucket name & S3 object key from SNS event
	s3Event, err := getS3EventFromSNSEvent(snsEvent)
	if err != nil {
        log.Fatalln(err)
        return err
	}
	s3Bucket, s3ObjectKey, err := parseS3Event(s3Event)
	if err != nil {
        log.Fatalln(err)
        return err
	}
	log.Printf("S3 upload event received from SNS: Bucket = %s, Object = %s\n", s3Bucket, s3ObjectKey)

	// Open connection to DB & validate with a ping
	db, err := sqlx.Connect(dbDriverName, dbDataSourceName)
	if err != nil {
		log.Fatalln(err)
		return err
	}
	log.Println("DB connection successful!")

	// Insert record in DB, with schema:
	// CREATE TABLE s3_objects (
	//     bucket varchar(40),
	//     object_key varchar(40)
	// );
	tx := db.MustBegin()
	tx.MustExec("INSERT INTO s3_objects (bucket, object_key) VALUES ($1, $2)", s3Bucket, s3ObjectKey)
	tx.Commit()

	return nil
}

func main() {
	lambda.Start(handler)
}
