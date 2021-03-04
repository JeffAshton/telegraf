package d2lkinesis

import (
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func createKinesisRecordSet(
	records []*kinesis.PutRecordsRequestEntry,
) kinesisRecordIterator {

	return &kinesisRecordSet{
		count:   len(records),
		index:   0,
		records: records,
	}
}

type kinesisRecordSet struct {
	kinesisRecordIterator

	count   int
	index   int
	records []*kinesis.PutRecordsRequestEntry
}

func (s *kinesisRecordSet) Next() (*kinesis.PutRecordsRequestEntry, error) {

	index := s.index
	if index >= s.count {
		return nil, nil
	}

	s.index++
	return s.records[index], nil
}
