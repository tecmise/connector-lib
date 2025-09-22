package client_rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/tecmise/connector-lib/pkg/ports/output/connector"
	"github.com/valyala/fasthttp"
	"io"
	"mime/multipart"
	"strings"
)

func NewClient[T any, R any](serviceName string) RestProxyProtocolClient[T, R] {
	return &protocolClient[T, R]{
		serviceName: serviceName,
	}
}

type (
	protocolClient[Request any, Response any] struct {
		serviceName string
	}

	RestProxyProtocolClient[Request any, Response any] interface {
		GET(ctx context.Context, resource string, headers map[string]string) (*Response, error)
		POST(ctx context.Context, resource string, body *Request, headers map[string]string) (*Response, error)
		PUT(ctx context.Context, resource string, body *Request, headers map[string]string) (*Response, error)
		PATCH(ctx context.Context, resource string, body *Request, headers map[string]string) (*Response, error)
		DELETE(ctx context.Context, resource string, body *Request, headers map[string]string) (*Response, error)
	}

	requestObject struct {
		Resource string            `json:"resource"`
		Method   string            `json:"method"`
		Body     interface{}       `json:"body,omitempty"`
		Headers  map[string]string `json:"headers,omitempty"`
		Host     string            `json:"host"`
	}
)

func (r *requestObject) getUrl() string {
	return fmt.Sprintf("%s/%s", r.Host, r.Resource)
}

func (p protocolClient[Request, Response]) GET(ctx context.Context, resource string, headers map[string]string) (*Response, error) {
	var response Response
	return &response, sendRequest(ctx, requestObject{
		Resource: resource,
		Method:   "GET",
		Body:     nil,
		Headers:  headers,
		Host:     p.serviceName,
	}, &response)
}

func (p protocolClient[Request, Response]) POST(ctx context.Context, resource string, body *Request, headers map[string]string) (*Response, error) {
	var response Response
	return &response, sendRequest(ctx, requestObject{
		Resource: resource,
		Method:   "POST",
		Body:     body,
		Headers:  headers,
		Host:     p.serviceName,
	}, &response)
}

func (p protocolClient[Request, Response]) PUT(ctx context.Context, resource string, body *Request, headers map[string]string) (*Response, error) {
	var response Response
	return &response, sendRequest(ctx, requestObject{
		Resource: resource,
		Method:   "PUT",
		Body:     body,
		Headers:  headers,
		Host:     p.serviceName,
	}, &response)
}

func (p protocolClient[Request, Response]) PATCH(ctx context.Context, resource string, body *Request, headers map[string]string) (*Response, error) {
	var response Response
	return &response, sendRequest(ctx, requestObject{
		Resource: resource,
		Method:   "PATCH",
		Body:     body,
		Headers:  headers,
		Host:     p.serviceName,
	}, &response)
}

func (p protocolClient[Request, Response]) DELETE(ctx context.Context, resource string, body *Request, headers map[string]string) (*Response, error) {
	var response Response
	return &response, sendRequest(ctx, requestObject{
		Resource: resource,
		Method:   "DELETE",
		Body:     body,
		Headers:  headers,
		Host:     p.serviceName,
	}, &response)
}

func sendRequest(ctx context.Context, param requestObject, response interface{}) error {
	logrus.Debugf("[connector] Resource: %s\n", param.Resource)
	logrus.Debugf("[connector] Host: %s\n", param.Host)
	logrus.Debugf("[connector] Body: %v\n", param.Body)
	logrus.Debugf("[connector] Method: %s\n", strings.ToUpper(param.Method))

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	method := strings.ToUpper(param.Method)
	if strings.HasPrefix(param.Resource, "/") {
		return fmt.Errorf("resource invalid")
	}

	uri := param.getUrl()
	req.SetRequestURI(uri)
	req.Header.SetMethod(method)
	req.Header.Set("Accept", "application/json")

	isFormData := false

	for key, value := range param.Headers {
		logrus.Debugf("[connector] Header: %s: %s\n", key, value)
		req.Header.Set(key, value)
		if key == "Content-Type" {
			if strings.HasPrefix(value, "multipart/form-data") {
				isFormData = true

			}
		}
	}

	if method == "POST" || method == "PUT" || method == "PATCH" || method == "DELETE" {
		if isFormData {
			var b bytes.Buffer
			writer := multipart.NewWriter(&b)

			if form, ok := param.Body.(*multipart.Form); ok {
				for key, vals := range form.Value {
					for _, val := range vals {
						if err := writer.WriteField(key, val); err != nil {
							return fmt.Errorf("error writing form field %s: %w", key, err)
						}
					}
				}

				for key, files := range form.File {
					for _, fh := range files {
						fileWriter, err := writer.CreateFormFile(key, fh.Filename)
						if err != nil {
							return fmt.Errorf("error creating form file %s: %w", key, err)
						}

						file, err := fh.Open()
						if err != nil {
							return fmt.Errorf("error opening file %s: %w", key, err)
						}
						defer file.Close()

						if _, err := io.Copy(fileWriter, file); err != nil {
							return fmt.Errorf("error copying file %s: %w", key, err)
						}
					}
				}
			}

			writer.Close()
			req.SetBody(b.Bytes())
			req.Header.Set("Content-Type", writer.FormDataContentType())
		} else {
			requestBody, err := json.Marshal(param.Body)
			if err != nil {
				return fmt.Errorf("error marshaling request body: %w", err)
			}
			req.SetBody(requestBody)
		}

	}

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	err := fasthttp.Do(req, resp)
	if err != nil {
		return err
	}

	logrus.Debugf("Response status code: %d\n", resp.StatusCode())
	if resp.Body() != nil {
		logrus.Debugf("Response body: %s\n", resp.Body)
	}

	if resp.StatusCode() == 204 {
		return nil
	}

	if resp.StatusCode() >= 200 && resp.StatusCode() < 300 {
		err := json.Unmarshal(resp.Body(), &response)
		if err != nil {
			return err
		}
		return nil
	}

	var errResponse connector.Result[string]
	err = json.Unmarshal(resp.Body(), &errResponse)
	if err != nil {
		return err
	}
	return errors.New(errResponse.Content)
}
