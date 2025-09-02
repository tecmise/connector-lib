package lambda

type (
	Headers struct {
		Accept        string `json:"Accept"`
		Authorization string `json:"Authorization"`
		ContentType   string `json:"content-type"`
		XApiKey       string `json:"x-api-key"`
		XUserPool     string `json:"X-user-pool"`
	}

	MultiValuesHeaders struct {
		Accept        []string `json:"Accept"`
		Authorization []string `json:"Authorization"`
		ContentType   []string `json:"content-type"`
		XApiKey       []string `json:"x-api-key"`
		XUserPool     []string `json:"X-user-pool"`
	}

	RequestContext struct {
		ResourcePath string `json:"resourcePath"`
		Path         string `json:"path"`
		HttpMethod   string `json:"httpMethod"`
	}
)
