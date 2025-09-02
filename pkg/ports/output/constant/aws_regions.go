package constant

// AWSRegion representa uma região da AWS
type AWSRegion string

const (
	USEast1      AWSRegion = "us-east-1"
	USEast2      AWSRegion = "us-east-2"
	USWest1      AWSRegion = "us-west-1"
	USWest2      AWSRegion = "us-west-2"
	EUWest1      AWSRegion = "eu-west-1"
	EUWest2      AWSRegion = "eu-west-2"
	EUWest3      AWSRegion = "eu-west-3"
	EUNorth1     AWSRegion = "eu-north-1"
	EUSouth1     AWSRegion = "eu-south-1"
	APSouth1     AWSRegion = "ap-south-1"
	APSouth2     AWSRegion = "ap-south-2"
	APNortheast1 AWSRegion = "ap-northeast-1"
	APNortheast2 AWSRegion = "ap-northeast-2"
	APNortheast3 AWSRegion = "ap-northeast-3"
	APSoutheast1 AWSRegion = "ap-southeast-1"
	APSoutheast2 AWSRegion = "ap-southeast-2"
	APCentral1   AWSRegion = "ap-central-1"
	SAMethod1    AWSRegion = "sa-east-1"
	CAForth1     AWSRegion = "ca-central-1"
	AFSouth1     AWSRegion = "af-south-1"
	MECentral1   AWSRegion = "me-central-1"
	MESouth1     AWSRegion = "me-south-1"
)

func (r AWSRegion) String() string {
	return string(r)
}

// IsValid verifica se a região fornecida é válida
func (r AWSRegion) IsValid() bool {
	switch r {
	case USEast1, USEast2, USWest1, USWest2,
		EUWest1, EUWest2, EUWest3, EUNorth1, EUSouth1,
		APSouth1, APSouth2, APNortheast1, APNortheast2, APNortheast3,
		APSoutheast1, APSoutheast2, APCentral1, SAMethod1,
		CAForth1, AFSouth1, MECentral1, MESouth1:
		return true
	default:
		return false
	}
}
