package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tecmise/connector-api/pkg/ports/output/connector"
	"github.com/tecmise/connector-api/pkg/ports/output/constant"
	lambda2 "github.com/tecmise/connector-api/pkg/ports/output/lambda"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/sirupsen/logrus"
)

type (
	payload struct {
		Resource                        string                     `json:"resource"`
		Path                            string                     `json:"path"`
		HttpMethod                      string                     `json:"httpMethod"`
		Headers                         lambda2.Headers            `json:"headers"`
		MultiValueHeaders               lambda2.MultiValuesHeaders `json:"multiValueHeaders"`
		QueryStringParameters           interface{}                `json:"queryStringParameters"`
		MultiValueQueryStringParameters interface{}                `json:"multiValueQueryStringParameters"`
		PathParameters                  interface{}                `json:"pathParameters"`
		RequestContext                  lambda2.RequestContext     `json:"requestContext"`
		Body                            string                     `json:"body"`
	}

	Connector[T any] struct {
		parameter connector.Parameter
	}
)

func (c Connector[T]) Find(parameter connector.Parameter, response *T) error {
	return call(parameter, response)
}

func (c Connector[T]) List(parameter connector.Parameter, response *[]T) error {
	return call(parameter, response)
}

func (c Connector[T]) Page(parameter connector.Parameter, response *connector.ListResponse[T]) error {
	return call(parameter, response)
}

func (c Connector[T]) Ids(parameter connector.Parameter, response *[]int64) error {
	return call(parameter, response)
}

func (c Connector[T]) Strings(parameter connector.Parameter, response *[]string) error {
	return call(parameter, response)
}

func (c Connector[T]) Create(parameter connector.Parameter, response *T) error {
	return call(parameter, response)
}

func (c Connector[T]) Update(parameter connector.Parameter, response *T) error {
	return call(parameter, response)
}

func (c Connector[T]) Inative(parameter connector.Parameter, response *T) error {
	return call(parameter, response)
}

func call(parameter connector.Parameter, response interface{}) error {
	client, ok := LambdaClients[parameter.Region]
	if !ok {
		logrus.Errorf("Região %s não definida em LambdaClients", parameter.Region)
		return errors.New("region doesn't defined")
	}

	payloadBytes, err := json.Marshal(parameter.Body)
	if err != nil {
		logrus.Errorf("Erro ao serializar o body do parâmetro: %v", err)
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	var body string
	if parameter.Method == "POST" || parameter.Method == "PUT" {
		body = string(payloadBytes)
	}

	payloadData := payload{
		Resource:   parameter.Resource,
		Path:       parameter.Resource,
		HttpMethod: parameter.Method,
		Headers: lambda2.Headers{
			Accept:      "application/json",
			ContentType: "application/json",
			XUserPool:   parameter.UserPoolID,
		},
		MultiValueHeaders: lambda2.MultiValuesHeaders{
			Accept:      []string{"application/json"},
			ContentType: []string{"application/json"},
			XUserPool:   []string{parameter.UserPoolID},
		},
		PathParameters: nil,
		RequestContext: lambda2.RequestContext{
			ResourcePath: parameter.Resource,
			Path:         parameter.Resource,
			HttpMethod:   parameter.Method,
		},
		Body: body,
	}

	payloadJson, err := json.Marshal(payloadData)
	if err != nil {
		logrus.Errorf("Erro ao serializar o payload: %v", err)
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	logrus.Debugf("Payload JSON: %s", string(payloadJson))

	input := &lambda.InvokeInput{
		FunctionName: aws.String(parameter.Host),
		Payload:      payloadJson,
	}

	resp, err := client.Invoke(context.TODO(), input)
	if err != nil {
		logrus.Errorf("Falha ao invocar a Lambda: %v", err)
		return fmt.Errorf("failed to invoke lambda: %w", err)
	}

	if resp.FunctionError != nil {
		logrus.Errorf("Erro na função Lambda: %s", aws.ToString(resp.FunctionError))
		return fmt.Errorf("lambda error: %s", aws.ToString(resp.FunctionError))
	}

	logrus.Debugf("Lambda response status code: %d", resp.StatusCode)
	logrus.Debugf("Lambda response payload: %s", string(resp.Payload))

	if resp.StatusCode == 204 {
		logrus.Debugf("Lambda retornou status code 204 (No Content)")
		return nil
	}

	var result lambda2.Response
	err = json.Unmarshal(resp.Payload, &result)
	if err != nil {
		logrus.Errorf("Erro ao desserializar o payload de resposta da Lambda: %v", err)
		return err
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		err = json.Unmarshal([]byte(result.Body), &response)
		return nil
	}

	var errResponse connector.Result[string]
	err = json.Unmarshal(resp.Payload, &errResponse)
	if err != nil {
		logrus.Errorf("Erro ao desserializar a resposta de erro da Lambda: %v", err)
		return err
	}
	logrus.Errorf("Chamada à Lambda retornou erro: %s", errResponse.Content)
	return errors.New(errResponse.Content)
}

func NewConnector[T any]() connector.Call[T] {
	return Connector[T]{}
}

var (
	LambdaClients = map[constant.AWSRegion]*lambda.Client{}
)

func init() {
	construct := func(region constant.AWSRegion) *lambda.Client {
		cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region.String()))
		if err != nil {
			log.Fatalf("failed to load AWS configuration: %v", err)
		}
		return lambda.NewFromConfig(cfg)
	}
	LambdaClients[constant.USEast1] = construct(constant.USEast1)
}
