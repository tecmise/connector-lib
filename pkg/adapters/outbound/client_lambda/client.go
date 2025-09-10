package client_lambda

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/sirupsen/logrus"
)

type (
	LambdaClient[T any, R any] struct {
		client *lambda.Client
	}

	LambdaProtocolClient[T any, R any] interface {
		Invoke(ctx context.Context, lambdaName string, _body T) (*R, error)
	}
)

func NewLambdaRestProxyClient[T any, R any](lambdaClient *lambda.Client) LambdaProtocolClient[T, R] {
	return &LambdaClient[T, R]{
		client: lambdaClient,
	}
}

func (c *LambdaClient[T, R]) Invoke(ctx context.Context, lambdaName string, _body T) (*R, error) {
	payloadBytes, err := json.Marshal(_body)
	if err != nil {
		logrus.Warnf("Failed to marshal payload: %v", err)
		return nil, err
	}

	resp, err := c.client.Invoke(ctx, &lambda.InvokeInput{
		FunctionName: aws.String(lambdaName),
		Payload:      payloadBytes,
	})

	if err != nil {
		logrus.Warnf("Failed to invoke lambda %s: %v", lambdaName, err)
		return nil, err
	}

	if resp.FunctionError != nil {
		logrus.Warnf("Failed to invoke lambda %s: %v", lambdaName, *resp.FunctionError)
		return nil, errors.New(*resp.FunctionError)
	}

	logrus.Debugf("Lambda response status code: %d", resp.StatusCode)
	logrus.Debugf("Lambda response payload: %s", string(resp.Payload))

	var result R
	convertErr := json.Unmarshal(resp.Payload, &result)
	if convertErr != nil {
		logrus.Warnf("Failed to unmarshal payload: %v", err)
	}
	return &result, convertErr
}
