package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/sirupsen/logrus"
	"github.com/tecmise/connector-lib/pkg/ports/output/assync"
)

type (
	AssyncPublisher interface {
		Publish(ctx context.Context, req *assync.QueueRequest, queueUrl string) (*assync.QueueTriggerResponse, error)
	}

	assyncPublisher struct {
		client *sqs.Client
	}
)

func NewAssyncPublisher() AssyncPublisher {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))
	if err != nil {
		logrus.Fatalf("unable to load SDK config, %v", err)
	}
	return &assyncPublisher{
		client: sqs.NewFromConfig(cfg),
	}
}

func (a assyncPublisher) Publish(ctx context.Context, req *assync.QueueRequest, queueUrl string) (*assync.QueueTriggerResponse, error) {
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
	return &assync.QueueTriggerResponse{
		MessageId: fmt.Sprintf("%s", *message.MessageId),
	}, nil
}
