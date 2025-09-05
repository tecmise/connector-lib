package lambda

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/sirupsen/logrus"
)

type InvokeOutputResult[R any] struct {
	Output *lambda.InvokeOutput
	Error  error
}

func (r InvokeOutputResult[R]) Marshal(response interface{}) error {
	if r.Error != nil {
		logrus.Errorf(r.Error.Error())
		return r.Error
	}

	resp := r.Output
	if resp.StatusCode == 204 {
		logrus.Debugf("Lambda retornou status code 204 (No Content)")
		return nil
	}

	var result Response
	err := json.Unmarshal(resp.Payload, &result)
	if err != nil {
		logrus.Errorf("Erro ao desserializar o payload de resposta da Lambda: %v", err)
		return err
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		err = json.Unmarshal([]byte(result.Body), &response)
		if err != nil {
			logrus.Errorf("Erro ao desserializar a resposta de erro da Lambda: %v", err)
			return err
		}
	}
	return nil
}
