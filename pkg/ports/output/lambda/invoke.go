package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/sirupsen/logrus"
	"net/http"
)

type (
	ProtocolClient[T any, R any] struct {
		host   string
		path   string
		client *lambda.Client
	}
)

func (c *ProtocolClient[T, R]) GET(ctx context.Context) InvokeOutputResult[R] {
	return c.invoke(ctx, nil, http.MethodGet)
}

func (c *ProtocolClient[T, R]) POST(ctx context.Context, body *T) InvokeOutputResult[R] {
	return c.invoke(ctx, body, http.MethodPost)
}

func (c *ProtocolClient[T, R]) PUT(ctx context.Context, body *T) InvokeOutputResult[R] {
	return c.invoke(ctx, body, http.MethodPut)
}

func (c *ProtocolClient[T, R]) PATCH(ctx context.Context, body *T) InvokeOutputResult[R] {
	return c.invoke(ctx, body, http.MethodPatch)
}

func (c *ProtocolClient[T, R]) DELETE(ctx context.Context, body *T) InvokeOutputResult[R] {
	return c.invoke(ctx, body, http.MethodDelete)
}

func (c *ProtocolClient[T, R]) invoke(
	ctx context.Context,
	_body interface{},
	method string,
) InvokeOutputResult[R] {
	payloadBytes, err := json.Marshal(_body)
	if err != nil {
		logrus.Errorf("Erro ao serializar o body do parâmetro: %v", err)
		return InvokeOutputResult[R]{
			output: nil,
			error:  err,
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

	payloadData := Payload{
		Resource:          c.path,
		Path:              c.path,
		HttpMethod:        method,
		Headers:           headers,
		MultiValueHeaders: map[string][]string{},
		PathParameters:    nil,
		RequestContext: RequestContext{
			ResourcePath: c.path,
			Path:         c.path,
			HttpMethod:   method,
		},
		Body: body,
	}

	payloadJson, err := json.Marshal(payloadData)
	if err != nil {
		logrus.Errorf("Erro ao serializar o payload: %v", err)
		return InvokeOutputResult[R]{
			output: nil,
			error:  err,
		}
	}

	logrus.Debugf("Payload JSON: %s", string(payloadJson))

	input := &lambda.InvokeInput{
		FunctionName: aws.String(c.host),
		Payload:      payloadJson,
	}

	resp, err := c.client.Invoke(context.TODO(), input)
	if err != nil {
		logrus.Errorf("Falha ao invocar a Lambda: %v", err)
		return InvokeOutputResult[R]{
			output: nil,
			error:  err,
		}
	}

	if resp.FunctionError != nil {
		logrus.Errorf("Erro na função Lambda: %s", aws.ToString(resp.FunctionError))
		return InvokeOutputResult[R]{
			output: nil,
			error:  err,
		}
	}

	logrus.Debugf("Lambda response status code: %d", resp.StatusCode)
	logrus.Debugf("Lambda response payload: %s", string(resp.Payload))

	return InvokeOutputResult[R]{
		output: resp,
		error:  nil,
	}
}
