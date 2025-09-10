package client_rest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tecmise/connector-lib/pkg/ports/output/connector"
	"io"
	"mime/multipart"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

type (
	Connector[T any] struct {
		parameter connector.Parameter
	}
)

func NewConnector[T any]() connector.Call[T] {
	return Connector[T]{}
}

func (c Connector[T]) Strings(parameter connector.Parameter, response *[]string) error {
	return call(parameter, response)
}

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
	logrus.Debugf("Calling REST API\n")
	logrus.Debugf("Resource: %s\n", parameter.Resource)
	logrus.Debugf("Host: %s\n", parameter.Host)
	logrus.Debugf("Body: %v\n", parameter.Body)
	logrus.Debugf("Method: %s\n", strings.ToUpper(parameter.Method))

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	method := strings.ToUpper(parameter.Method)
	if strings.HasPrefix(parameter.Resource, "/") {
		return fmt.Errorf("resource invalid")
	}

	uri := parameter.GetHttpURL()
	req.SetRequestURI(uri)
	req.Header.SetMethod(method)
	req.Header.Set("Accept", "application/json")

	req.Header.Set("Authorization", parameter.Headers["Authorization"])
	req.Header.Set("x-api-key", parameter.Headers["x-api-key"])

	isFormData := false

	if v, ok := parameter.Headers["Content-Type"]; ok {
		if strings.HasPrefix(v, "multipart/form-data") {
			req.Header.Set("Content-Type", "multipart/form-data")
			isFormData = true
		} else {
			req.Header.Set("Content-Type", "application/json")
		}
	} else {
		req.Header.Set("Content-Type", "application/json") // default
	}

	req.Header.Set("X-authenticated-user", parameter.UserID.String())
	req.Header.Set("X-user-pool", parameter.UserPoolID)

	logHeaders(&req.Header)

	if method == "POST" || method == "PUT" || method == "PATCH" || method == "DELETE" {

		if isFormData {
			var b bytes.Buffer
			writer := multipart.NewWriter(&b)

			if form, ok := parameter.Body.(*multipart.Form); ok {
				for key, vals := range form.Value {
					for _, val := range vals {
						if err := writer.WriteField(key, val); err != nil {
							return fmt.Errorf("error writing form field %s: %w", key, err)
						}
					}
				}

				// Adiciona arquivos se houver
				for key, files := range form.File {
					for _, fh := range files {
						fileWriter, err := writer.CreateFormFile(key, fh.Filename)
						if err != nil {
							return fmt.Errorf("error creating form file %s: %w", key, err)
						}

						// abre o arquivo
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
			requestBody, err := json.Marshal(parameter.Body)
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

func Call[T any](parameter *connector.Parameter, response *T) error {
	logrus.Debugf("Calling REST API\n")
	logrus.Debugf("Resource: %s\n", parameter.Resource)
	logrus.Debugf("Host: %s\n", parameter.Host)
	logrus.Debugf("Body: %v\n", parameter.Body)
	logrus.Debugf("Method: %s\n", strings.ToUpper(parameter.Method))

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	method := strings.ToUpper(parameter.Method)
	if strings.HasPrefix(parameter.Resource, "/") {
		return fmt.Errorf("resource invalid")
	}

	uri := parameter.GetHttpURL()
	req.SetRequestURI(uri)
	req.Header.SetMethod(method)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-authenticated-user", parameter.UserID.String())
	req.Header.Set("X-user-pool", parameter.UserPoolID)

	logHeaders(&req.Header)

	if method == "POST" || method == "PUT" || method == "PATCH" || method == "DELETE" {
		requestBody, err := json.Marshal(parameter.Body)
		if err != nil {
			return fmt.Errorf("error marshaling request body: %w", err)
		}
		req.SetBody(requestBody)
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
	var result connector.Result[T]

	if resp.StatusCode() >= 200 && resp.StatusCode() < 300 {
		err := json.Unmarshal(resp.Body(), &result)
		if err != nil {
			return err
		}
		*response = result.Content
		return nil
	}
	var errResponse connector.Result[string]
	err = json.Unmarshal(resp.Body(), &errResponse)
	if err != nil {
		return err
	}
	return errors.New(errResponse.Content)
}

func logHeaders(headers *fasthttp.RequestHeader) {
	if headers != nil {
		headers.VisitAll(func(key, value []byte) {
			logrus.Debugf("Header: %s: %s\n", key, value)
		})
	}
}
