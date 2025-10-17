package client_sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/sirupsen/logrus"
	"github.com/tecmise/connector-lib/pkg/ports/output/assync"
	"github.com/tecmise/connector-lib/pkg/ports/output/request"
)

type (
	AssyncPublisher interface {
		Publish(ctx context.Context, req request.Validatable, queueUrl, messageGroupId, messageDeduplicationId string) (*assync.QueueTriggerResponse, error)
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
func (a assyncPublisher) Publish(ctx context.Context, req request.Validatable, queueUrl, messageGroupId, messageDeduplicationId string) (*assync.QueueTriggerResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	queueURL := queueUrl
	content, err := json.Marshal(req)
	if err != nil {
		logrus.Fatal("Error", err)
		return nil, err
	}

	message, err := a.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(queueURL),
		MessageBody:            aws.String(string(content)),
		MessageGroupId:         aws.String(messageGroupId),
		MessageDeduplicationId: aws.String(messageDeduplicationId),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"kind": {
				DataType:    aws.String("String"),
				StringValue: aws.String(fmt.Sprintf("%T", req)),
			},
		},
	})

	if err != nil {
		logrus.Fatal("Error", err)
		return nil, err
	}
	return &assync.QueueTriggerResponse{
		MessageId: fmt.Sprintf("%s", *message.MessageId),
	}, nil
}
