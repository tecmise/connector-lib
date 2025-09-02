package lambda

type (
	Response struct {
		StatusCode        int         `json:"statusCode"`
		Headers           interface{} `json:"headers"`
		MultiValueHeaders struct {
			ContentType []string `json:"Content-Type"`
		} `json:"multiValueHeaders"`
		Body string `json:"body"`
	}
)
