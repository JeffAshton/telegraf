package d2lkinesis

import (
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type kinesisRecordIterator interface {
	Next() (*kinesis.PutRecordsRequestEntry, error)
}
