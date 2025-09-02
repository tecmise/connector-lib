package connector

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/tecmise/connector-lib/pkg/ports/output/constant"

	"github.com/gofrs/uuid"
)

type Parameter struct {
	Host       string
	Resource   string
	Method     string
	Body       any
	Region     constant.AWSRegion
	UserID     uuid.UUID
	UserPoolID string
	Headers    map[string]string
}

type ParameterBuilder struct {
	param Parameter
}

func (b *Parameter) GetHttpURL() string {
	return fmt.Sprintf("%s/%s", b.Host, b.Resource)
}

func NewParameterBuilder() *ParameterBuilder {
	return &ParameterBuilder{param: Parameter{}}
}

func (b *ParameterBuilder) WithHost(host string) *ParameterBuilder {
	b.param.Host = host
	return b
}

func (b *ParameterBuilder) WithResource(resource string) *ParameterBuilder {
	b.param.Resource = resource
	return b
}

func (b *ParameterBuilder) WithMethod(method string) *ParameterBuilder {
	b.param.Method = method
	return b
}

func (b *ParameterBuilder) WithBody(body any) *ParameterBuilder {
	b.param.Body = body
	return b
}

func (b *ParameterBuilder) WithRegion(region constant.AWSRegion) *ParameterBuilder {
	b.param.Region = region
	return b
}

func (b *ParameterBuilder) WithHeader(key, value string) *ParameterBuilder {
	if b.param.Headers == nil {
		b.param.Headers = make(map[string]string)
	}
	b.param.Headers[key] = value
	return b
}

func (b *ParameterBuilder) WithCredentials(ctx context.Context) *ParameterBuilder {
	token := ctx.Value("bearer-token")
	xApiKey := ctx.Value("x-api-key")
	if token == nil {
		logrus.Warnf("Token is null in context!")
	}
	if xApiKey == nil {
		logrus.Warnf("X api key is null!")
	}
	if token != nil {
		b.WithHeader("Authorization", fmt.Sprintf("Bearer %s", token.(string)))
	}
	if xApiKey != nil {
		b.WithHeader("x-api-key", xApiKey.(string))
	}
	return b
}

func (b *ParameterBuilder) Build() Parameter {
	if b.param.Region == "" {
		b.param.Region = constant.USEast1
	}
	return b.param
}
