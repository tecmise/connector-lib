package parameters

import "os"

var ConnectorType Variable = Variable(os.Getenv("SCHOOL_CONNECTOR_TYPE"))

type Variable string

func (v Variable) String() string {
	if v == "" {
		return "LAMBDA"
	}
	return string(v)
}

func (v Variable) IsLambda() bool {
	return v.String() == "LAMBDA"
}

func (v Variable) IsRest() bool {
	return v.String() == "REST"
}
