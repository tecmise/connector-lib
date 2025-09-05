package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/sirupsen/logrus"
	lambda2 "github.com/tecmise/connector-lib/pkg/ports/output/lambda"
	"net/http"
)

type (
	ProtocolClient[T any, R any] struct {
		lambdaName string
		uri        string
		client     *lambda.Client
	}
)

func NewClientV2[T any, R any](lambdaClient *lambda.Client, lambdaName string, uri string) *ProtocolClient[T, R] {
	return &ProtocolClient[T, R]{
		lambdaName: lambdaName,
		client:     lambdaClient,
		uri:        uri,
	}
}

func (c *ProtocolClient[T, R]) GET(ctx context.Context) lambda2.InvokeOutputResult[R] {
	return c.invoke(ctx, nil, http.MethodGet)
}

func (c *ProtocolClient[T, R]) POST(ctx context.Context, body *T) lambda2.InvokeOutputResult[R] {
	return c.invoke(ctx, body, http.MethodPost)
}

func (c *ProtocolClient[T, R]) PUT(ctx context.Context, body *T) lambda2.InvokeOutputResult[R] {
	return c.invoke(ctx, body, http.MethodPut)
}

func (c *ProtocolClient[T, R]) PATCH(ctx context.Context, body *T) lambda2.InvokeOutputResult[R] {
	return c.invoke(ctx, body, http.MethodPatch)
}

func (c *ProtocolClient[T, R]) DELETE(ctx context.Context, body *T) lambda2.InvokeOutputResult[R] {
	return c.invoke(ctx, body, http.MethodDelete)
}

func (c *ProtocolClient[T, R]) invoke(
	ctx context.Context,
	_body interface{},
	method string,
) lambda2.InvokeOutputResult[R] {
	payloadBytes, err := json.Marshal(_body)
	if err != nil {
		logrus.Errorf("Erro ao serializar o body do parâmetro: %v", err)
		return lambda2.InvokeOutputResult[R]{
			Output: nil,
			Error:  err,
		}
	}

	var body string
	if method == "POST" || method == "PUT" {
		body = string(payloadBytes)
	}

	token := ctx.Value("bearer-token")
	xApiKey := ctx.Value("x-api-key")
	headers := make(map[string]string)

	if token == nil {
		logrus.Warnf("Token is null in context!")
	} else {
		headers["Authorization"] = fmt.Sprintf("Bearer %s", token.(string))
	}

	if xApiKey == nil {
		logrus.Warnf("X api key is null!")
	} else {
		headers["x-api-key"] = xApiKey.(string)
	}

	payloadData := lambda2.Payload{
		Resource:          c.uri,
		Path:              c.uri,
		HttpMethod:        method,
		Headers:           headers,
		MultiValueHeaders: map[string][]string{},
		PathParameters:    nil,
		RequestContext: lambda2.RequestContext{
			ResourcePath: c.uri,
			Path:         c.uri,
			HttpMethod:   method,
		},
		Body: body,
	}

	payloadJson, err := json.Marshal(payloadData)
	if err != nil {
		logrus.Errorf("Erro ao serializar o payload: %v", err)
		return lambda2.InvokeOutputResult[R]{
			Output: nil,
			Error:  err,
		}
	}

	logrus.Debugf("Payload JSON: %s", string(payloadJson))

	input := &lambda.InvokeInput{
		FunctionName: aws.String(c.lambdaName),
		Payload:      payloadJson,
	}

	resp, err := c.client.Invoke(context.TODO(), input)
	if err != nil {
		logrus.Errorf("Falha ao invocar a Lambda: %v", err)
		return lambda2.InvokeOutputResult[R]{
			Output: nil,
			Error:  err,
		}
	}

	if resp.FunctionError != nil {
		logrus.Errorf("Erro na função Lambda: %s", aws.ToString(resp.FunctionError))
		return lambda2.InvokeOutputResult[R]{
			Output: nil,
			Error:  err,
		}
	}

	logrus.Debugf("Lambda response status code: %d", resp.StatusCode)
	logrus.Debugf("Lambda response payload: %s", string(resp.Payload))

	return lambda2.InvokeOutputResult[R]{
		Output: resp,
		Error:  nil,
	}
}
