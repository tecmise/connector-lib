package assync

type (
	QueueRequest interface {
		Validate() error
	}
)
