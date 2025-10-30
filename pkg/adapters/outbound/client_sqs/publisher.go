package client_sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/sirupsen/logrus"
	"github.com/tecmise/connector-lib/pkg/adapters/outbound/shared_kernel"
	"github.com/tecmise/connector-lib/pkg/ports/output/assync"
	"github.com/tecmise/connector-lib/pkg/ports/output/request"
	"strings"
)

type (
	AssyncPublisher interface {
		Publish(ctx context.Context, req request.Validatable, queueUrl string, fifoData *shared_kernel.FifoProperties) (*assync.QueueTriggerResponse, error)
	}

	assyncPublisher struct {
		client     *sqs.Client
		identifier string
	}
)

func NewAssyncPublisher(client *sqs.Client, identifier string) AssyncPublisher {
	return &assyncPublisher{
		client:     client,
		identifier: identifier,
	}
}
func (a assyncPublisher) Publish(ctx context.Context, req request.Validatable, queueUrl string, fifoData *shared_kernel.FifoProperties) (*assync.QueueTriggerResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	queueURL := queueUrl
	content, err := json.Marshal(req)
	if err != nil {
		logrus.Fatal("Error", err)
		return nil, err
	}

	isFifo := strings.HasSuffix(queueUrl, ".fifo")

	if fifoData != nil && !isFifo {
		return nil, fmt.Errorf("fifo data provided but queue URL is not a FIFO queue (missing .fifo suffix)")
	}
	if fifoData == nil && isFifo {
		return nil, fmt.Errorf("queue URL is FIFO but no fifo data provided")
	}

	input := sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(content)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"kind": {
				DataType:    aws.String("String"),
				StringValue: aws.String(fmt.Sprintf("%T", req)),
			},
			"identifier": {
				DataType:    aws.String("String"),
				StringValue: aws.String(a.identifier),
			},
		},
	}

	if isFifo {
		input.MessageGroupId = aws.String(fifoData.MessageGroupId)
		if fifoData.MessageDeduplicationId != "" {
			input.MessageDeduplicationId = aws.String(fifoData.MessageDeduplicationId)
		}
	}

	message, err := a.client.SendMessage(ctx, &input)

	if err != nil {
		logrus.Fatal("Error", err)
		return nil, err
	}

	return &assync.QueueTriggerResponse{
		MessageId: fmt.Sprintf("%s", *message.MessageId),
	}, nil
}
