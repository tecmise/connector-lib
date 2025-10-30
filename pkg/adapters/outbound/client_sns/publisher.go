package client_sns

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/sirupsen/logrus"
	"github.com/tecmise/connector-lib/pkg/adapters/outbound/shared_kernel"
	"github.com/tecmise/connector-lib/pkg/ports/output/assync"
	"github.com/tecmise/connector-lib/pkg/ports/output/request"
	"strings"
)

type (
	AssyncPublisherSns interface {
		Publish(ctx context.Context, req request.Validatable, topicArn, subject string, fifoData *shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error)
	}

	assyncPublisherSns struct {
		client     *sns.Client
		identifier string
	}
)

func NewPublisher(client *sns.Client, identifier string) AssyncPublisherSns {
	return &assyncPublisherSns{
		client:     client,
		identifier: identifier,
	}
}

func (a assyncPublisherSns) Publish(ctx context.Context, req request.Validatable, topicArn, subject string, fifoData *shared_kernel.FifoProperties) (*assync.SnsTriggerResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	content, err := json.Marshal(req)
	if err != nil {
		logrus.Error("error marshaling request:", err)
		return nil, err
	}

	isFifo := strings.HasSuffix(topicArn, ".fifo")

	if fifoData != nil && !isFifo {
		return nil, fmt.Errorf("fifo data provided but queue URL is not a FIFO queue (missing .fifo suffix)")
	}
	if fifoData == nil && isFifo {
		return nil, fmt.Errorf("queue URL is FIFO but no fifo data provided")
	}

	input := &sns.PublishInput{
		TopicArn: aws.String(topicArn),
		Subject:  aws.String(subject),
		Message:  aws.String(string(content)),
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

	message, err := a.client.Publish(ctx, input)
	if err != nil {
		logrus.Error("error sending message:", err)
		return nil, err
	}

	return &assync.SnsTriggerResponse{
		MessageId: aws.ToString(message.MessageId),
	}, nil
}
