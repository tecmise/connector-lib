package lambda

type (
	Payload struct {
		Resource                        string              `json:"resource"`
		Path                            string              `json:"path"`
		HttpMethod                      string              `json:"httpMethod"`
		Headers                         map[string]string   `json:"headers"`
		MultiValueHeaders               map[string][]string `json:"multiValueHeaders"`
		QueryStringParameters           interface{}         `json:"queryStringParameters"`
		MultiValueQueryStringParameters interface{}         `json:"multiValueQueryStringParameters"`
		PathParameters                  interface{}         `json:"pathParameters"`
		RequestContext                  RequestContext      `json:"requestContext"`
		Body                            string              `json:"body"`
	}

	//RequestContext struct {
	//	ResourcePath string `json:"resourcePath"`
	//	Path         string `json:"path"`
	//	HttpMethod   string `json:"httpMethod"`
	//}
)
