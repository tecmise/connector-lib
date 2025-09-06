package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/sirupsen/logrus"

	//"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/tecmise/connector-lib/pkg/ports/output/shared_kernel/queue_response"
)

type (
	AssyncPublisher[T any] interface {
		Publish(ctx context.Context, req *T, queueUrl string) (*queue_response.QueueTriggerResponse, error)
	}

	assyncPublisher[T any] struct {
		client *sqs.Client
	}
)

func NewAssyncPublisher[T any]() AssyncPublisher[T] {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))
	if err != nil {
		logrus.Fatalf("unable to load SDK config, %v", err)
	}
	return &assyncPublisher[T]{
		client: sqs.NewFromConfig(cfg),
	}
}

func (a assyncPublisher[T]) Publish(ctx context.Context, req *T, queueUrl string) (*queue_response.QueueTriggerResponse, error) {
	queueURL := queueUrl
	content, err := json.Marshal(req)
	if err != nil {
		logrus.Fatal("Error", err)
		return nil, err
	}
	message, err := a.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(content)),
	})
	if err != nil {
		logrus.Fatal("Error", err)
		return nil, err
	}
	return &queue_response.QueueTriggerResponse{
		MessageId: fmt.Sprintf("%s", *message.MessageId),
	}, nil
}
