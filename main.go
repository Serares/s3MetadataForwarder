package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type S3RecordBucket struct {
	Name string `json:name`
}

type S3RecordObject struct {
	Key string `json:key`
}

type S3Event struct {
	Records []S3Object `json:Records`
}

type S3Object struct {
	S3 S3Record `json:s3`
}

type S3Record struct {
	Bucket S3RecordBucket `json:bucket`
	Object S3RecordObject `json:object`
}

// Define your AWS region
const awsRegion = "eu-central-1" // Change to your desired region

func main() {
	queueUrl := os.Getenv("PROCESS_QUEUE_URL")
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(awsRegion))
	if err != nil {
		log.Fatalf("Error loading AWS config: %v", err)
	}

	// Create an S3 client
	// s3Client := s3.NewFromConfig(cfg)
	sqsClient := sqs.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg)

	// Continuously poll for S3 events from EventBridge
	for {
		events, err := pollSqsMessages(ctx, sqsClient, queueUrl)
		if err != nil {
			log.Printf("Error getting EventBridge events: %v", err)
			continue
		}

		for _, event := range events {
			bs, _ := json.Marshal(event)
			// Extract S3 bucket and object key from the event
			log.Println("Event received from sqs", string(bs))

			S3Event := &S3Event{}
			err := json.Unmarshal([]byte(*event.Body), &S3Event)
			if err != nil {
				log.Println("Failed to unmarshal the event")
			}
			s3Obj, _ := getObjectMetadataFromS3(ctx, s3Client, S3Event)
			for key, value := range s3Obj {
				log.Printf("Key: %s, Value: %s\n", key, value)
			}

			deleted, _ := deleteSqsMessage(ctx, sqsClient, queueUrl, event)
			log.Println("Message deleted success", deleted)

		}
	}
}

// Function to fetch S3 object metadata
func getObjectMetadataFromS3(ctx context.Context, s3Client *s3.Client, event *S3Event) (map[string]string, error) {
	result, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &event.Records[0].S3.Bucket.Name,
		Key:    &event.Records[0].S3.Object.Key,
	})

	if err != nil {
		log.Printf("Couldn't get object %v:%v. Here's why: %v\n", &event.Records[0].S3.Bucket.Name, &event.Records[0].S3.Object.Key, err)
		return nil, err
	}

	defer result.Body.Close()

	return result.Metadata, nil
}

func pollSqsMessages(ctx context.Context, sqsClient *sqs.Client, queueURL string) ([]types.Message, error) {
	// Create an SQS service client
	receiveMsgInput := &sqs.ReceiveMessageInput{
		QueueUrl:            &queueURL,
		MaxNumberOfMessages: 10,
	}

	resp, err := sqsClient.ReceiveMessage(ctx, receiveMsgInput)
	if err != nil {
		return nil, err
	}

	return resp.Messages, nil
}

func deleteSqsMessage(ctx context.Context, sqsClient *sqs.Client, queueURL string, event types.Message) (string, error) {

	deleteMsgOutput, err := sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{ReceiptHandle: event.ReceiptHandle, QueueUrl: &queueURL})
	if err != nil {
		log.Printf("Error deleting the message %v", err)
	}
	log.Println("Message deleted success %v", deleteMsgOutput.ResultMetadata)
	return *event.ReceiptHandle, nil
}
