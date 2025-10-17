package client_sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/sirupsen/logrus"
	"github.com/tecmise/connector-lib/pkg/ports/output/assync"
	"github.com/tecmise/connector-lib/pkg/ports/output/request"
)

type (
	AssyncPublisherV2 interface {
		Publish(ctx context.Context, req request.Validatable, queueUrl, messageGroupId, messageDeduplicationId string) (*assync.QueueTriggerResponse, error)
	}

	assyncPublisherV2 struct {
		client *sqs.Client
	}
)

func NewAssyncPublisherV2(client *sqs.Client) AssyncPublisher {
	return &assyncPublisherV2{
		client: client,
	}
}
func (a assyncPublisherV2) Publish(ctx context.Context, req request.Validatable, queueUrl, messageGroupId, messageDeduplicationId string) (*assync.QueueTriggerResponse, error) {
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
