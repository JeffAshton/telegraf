package kinesis

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/gofrs/uuid"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/serializers"
	"github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/assert"
)

const zero int64 = 0

func TestPartitionKey(t *testing.T) {

	assert := assert.New(t)
	testPoint := testutil.TestMetric(1)

	k := KinesisOutput{
		Log: testutil.Logger{},
		Partition: &Partition{
			Method: "static",
			Key:    "-",
		},
	}
	assert.Equal("-", k.getPartitionKey(testPoint), "PartitionKey should be '-'")

	k = KinesisOutput{
		Log: testutil.Logger{},
		Partition: &Partition{
			Method: "tag",
			Key:    "tag1",
		},
	}
	assert.Equal(testPoint.Tags()["tag1"], k.getPartitionKey(testPoint), "PartitionKey should be value of 'tag1'")

	k = KinesisOutput{
		Log: testutil.Logger{},
		Partition: &Partition{
			Method:  "tag",
			Key:     "doesnotexist",
			Default: "somedefault",
		},
	}
	assert.Equal("somedefault", k.getPartitionKey(testPoint), "PartitionKey should use default")

	k = KinesisOutput{
		Log: testutil.Logger{},
		Partition: &Partition{
			Method: "tag",
			Key:    "doesnotexist",
		},
	}
	assert.Equal("telegraf", k.getPartitionKey(testPoint), "PartitionKey should be telegraf")

	k = KinesisOutput{
		Log: testutil.Logger{},
		Partition: &Partition{
			Method: "not supported",
		},
	}
	assert.Equal("", k.getPartitionKey(testPoint), "PartitionKey should be value of ''")

	k = KinesisOutput{
		Log: testutil.Logger{},
		Partition: &Partition{
			Method: "measurement",
		},
	}
	assert.Equal(testPoint.Name(), k.getPartitionKey(testPoint), "PartitionKey should be value of measurement name")

	k = KinesisOutput{
		Log: testutil.Logger{},
		Partition: &Partition{
			Method: "random",
		},
	}
	partitionKey := k.getPartitionKey(testPoint)
	u, err := uuid.FromString(partitionKey)
	assert.Nil(err, "Issue parsing UUID")
	assert.Equal(byte(4), u.Version(), "PartitionKey should be UUIDv4")

	k = KinesisOutput{
		Log:          testutil.Logger{},
		PartitionKey: "-",
	}
	assert.Equal("-", k.getPartitionKey(testPoint), "PartitionKey should be '-'")

	k = KinesisOutput{
		Log:                testutil.Logger{},
		RandomPartitionKey: true,
	}
	partitionKey = k.getPartitionKey(testPoint)
	u, err = uuid.FromString(partitionKey)
	assert.Nil(err, "Issue parsing UUID")
	assert.Equal(byte(4), u.Version(), "PartitionKey should be UUIDv4")
}

func TestWriteKinesis_WhenSuccess(t *testing.T) {

	assert := assert.New(t)

	partitionKey := "partitionKey"
	shard := "shard"
	sequenceNumber := "sequenceNumber"
	streamName := "stream"

	records := []*kinesis.PutRecordsRequestEntry{
		{
			PartitionKey: &partitionKey,
			Data:         []byte{0x65},
		},
	}

	svc := &mockKinesisPutRecords{}
	svc.SetupResponse(
		0,
		[]*kinesis.PutRecordsResultEntry{
			{
				ErrorCode:      nil,
				ErrorMessage:   nil,
				SequenceNumber: &sequenceNumber,
				ShardId:        &shard,
			},
		},
	)

	k := KinesisOutput{
		Log:        testutil.Logger{},
		StreamName: streamName,
		svc:        svc,
	}

	elapsed := k.writeKinesis(records)
	assert.GreaterOrEqual(elapsed.Nanoseconds(), zero)

	svc.AssertRequests(assert, []*kinesis.PutRecordsInput{
		{
			StreamName: &streamName,
			Records:    records,
		},
	})
}

func TestWriteKinesis_WhenRecordErrors(t *testing.T) {

	assert := assert.New(t)

	errorCode := "InternalFailure"
	errorMessage := "Internal Service Failure"
	partitionKey := "partitionKey"
	streamName := "stream"

	records := []*kinesis.PutRecordsRequestEntry{
		{
			PartitionKey: &partitionKey,
			Data:         []byte{0x66},
		},
	}

	svc := &mockKinesisPutRecords{}
	svc.SetupResponse(
		1,
		[]*kinesis.PutRecordsResultEntry{
			{
				ErrorCode:      &errorCode,
				ErrorMessage:   &errorMessage,
				SequenceNumber: nil,
				ShardId:        nil,
			},
		},
	)

	k := KinesisOutput{
		Log:        testutil.Logger{},
		StreamName: streamName,
		svc:        svc,
	}

	elapsed := k.writeKinesis(records)
	assert.GreaterOrEqual(elapsed.Nanoseconds(), zero)

	svc.AssertRequests(assert, []*kinesis.PutRecordsInput{
		{
			StreamName: &streamName,
			Records:    records,
		},
	})
}

func TestWriteKinesis_WhenServiceError(t *testing.T) {

	assert := assert.New(t)

	partitionKey := "partitionKey"
	streamName := "stream"

	records := []*kinesis.PutRecordsRequestEntry{
		{
			PartitionKey: &partitionKey,
			Data:         []byte{},
		},
	}

	svc := &mockKinesisPutRecords{}
	svc.SetupErrorResponse(
		awserr.New("InvalidArgumentException", "Invalid record", nil),
	)

	k := KinesisOutput{
		Log:        testutil.Logger{},
		StreamName: streamName,
		svc:        svc,
	}

	elapsed := k.writeKinesis(records)
	assert.GreaterOrEqual(elapsed.Nanoseconds(), zero)

	svc.AssertRequests(assert, []*kinesis.PutRecordsInput{
		{
			StreamName: &streamName,
			Records:    records,
		},
	})
}

func TestWrite_NoMetrics(t *testing.T) {

	assert := assert.New(t)

	svc := &mockKinesisPutRecords{}

	serializer := influx.NewSerializer()

	k := KinesisOutput{
		Log:                  testutil.Logger{},
		maxRecordsPerRequest: 500,
		Partition: &Partition{
			Method: "static",
			Key:    "partitionKey",
		},
		StreamName: "stream",
		serializer: serializer,
		svc:        svc,
	}

	err := k.Write([]telegraf.Metric{})
	assert.Nil(err, "Should not return error")

	svc.AssertRequests(assert, []*kinesis.PutRecordsInput{})
}

func TestWrite_SingleMetric_SingleRequest(t *testing.T) {

	assert := assert.New(t)

	partitionKey := "partitionKey"
	streamName := "stream"

	svc := &mockKinesisPutRecords{}
	svc.SetupSuccessfulResponse(1)

	serializer := influx.NewSerializer()

	k := KinesisOutput{
		Log:                  testutil.Logger{},
		maxRecordsPerRequest: 500,
		Partition: &Partition{
			Method: "static",
			Key:    partitionKey,
		},
		StreamName: streamName,
		serializer: serializer,
		svc:        svc,
	}

	metric := testutil.TestMetric(1)
	metricData := serializeMetric(serializer, metric, assert)

	err := k.Write([]telegraf.Metric{
		metric,
	})
	assert.Nil(err, "Should not return error")

	svc.AssertRequests(assert, []*kinesis.PutRecordsInput{
		{
			StreamName: &streamName,
			Records: []*kinesis.PutRecordsRequestEntry{
				{
					PartitionKey: &partitionKey,
					Data:         metricData,
				},
			},
		},
	})
}

func TestWrite_MultipleMetrics_SingleRequest(t *testing.T) {

	assert := assert.New(t)

	partitionKey := "partitionKey"
	streamName := "stream"

	svc := &mockKinesisPutRecords{}
	svc.SetupSuccessfulResponse(3)

	serializer := influx.NewSerializer()

	k := KinesisOutput{
		Log:                  testutil.Logger{},
		maxRecordsPerRequest: 500,
		Partition: &Partition{
			Method: "static",
			Key:    partitionKey,
		},
		StreamName: streamName,
		serializer: serializer,
		svc:        svc,
	}

	metric1 := testutil.TestMetric(1, "metric1")
	metric1Data := serializeMetric(serializer, metric1, assert)

	metric2 := testutil.TestMetric(2, "metric2")
	metric2Data := serializeMetric(serializer, metric2, assert)

	metric3 := testutil.TestMetric(3, "metric3")
	metric3Data := serializeMetric(serializer, metric3, assert)

	err := k.Write([]telegraf.Metric{
		metric1,
		metric2,
		metric3,
	})
	assert.Nil(err, "Should not return error")

	svc.AssertRequests(assert, []*kinesis.PutRecordsInput{
		{
			StreamName: &streamName,
			Records: []*kinesis.PutRecordsRequestEntry{
				{
					PartitionKey: &partitionKey,
					Data:         metric1Data,
				},
				{
					PartitionKey: &partitionKey,
					Data:         metric2Data,
				},
				{
					PartitionKey: &partitionKey,
					Data:         metric3Data,
				},
			},
		},
	})
}

func TestWrite_MultipleMetrics_MultipleRequests(t *testing.T) {

	assert := assert.New(t)

	partitionKey := "partitionKey"
	streamName := "stream"

	svc := &mockKinesisPutRecords{}
	svc.SetupSuccessfulResponse(2)
	svc.SetupSuccessfulResponse(1)

	serializer := influx.NewSerializer()

	k := KinesisOutput{
		Log:                  testutil.Logger{},
		maxRecordsPerRequest: 2,
		Partition: &Partition{
			Method: "static",
			Key:    partitionKey,
		},
		StreamName: streamName,
		serializer: serializer,
		svc:        svc,
	}

	metric1 := testutil.TestMetric(1, "metric1")
	metric1Data := serializeMetric(serializer, metric1, assert)

	metric2 := testutil.TestMetric(2, "metric2")
	metric2Data := serializeMetric(serializer, metric2, assert)

	metric3 := testutil.TestMetric(3, "metric3")
	metric3Data := serializeMetric(serializer, metric3, assert)

	err := k.Write([]telegraf.Metric{
		metric1,
		metric2,
		metric3,
	})
	assert.Nil(err, "Should not return error")

	svc.AssertRequests(assert, []*kinesis.PutRecordsInput{
		{
			StreamName: &streamName,
			Records: []*kinesis.PutRecordsRequestEntry{
				{
					PartitionKey: &partitionKey,
					Data:         metric1Data,
				},
				{
					PartitionKey: &partitionKey,
					Data:         metric2Data,
				},
			},
		},
		{
			StreamName: &streamName,
			Records: []*kinesis.PutRecordsRequestEntry{
				{
					PartitionKey: &partitionKey,
					Data:         metric3Data,
				},
			},
		},
	})
}

func TestWrite_SerializerError(t *testing.T) {

	assert := assert.New(t)

	partitionKey := "partitionKey"
	streamName := "stream"

	svc := &mockKinesisPutRecords{}
	svc.SetupSuccessfulResponse(2)

	serializer := influx.NewSerializer()

	k := KinesisOutput{
		Log:                  testutil.Logger{},
		maxRecordsPerRequest: 500,
		Partition: &Partition{
			Method: "static",
			Key:    partitionKey,
		},
		StreamName: streamName,
		serializer: serializer,
		svc:        svc,
	}

	metric1 := testutil.TestMetric(1, "metric1")
	metric1Data := serializeMetric(serializer, metric1, assert)

	metric2 := testutil.TestMetric(2, "metric2")
	metric2Data := serializeMetric(serializer, metric2, assert)

	// metric is invalid because of empty name
	invalidMetric := testutil.TestMetric(3, "")

	err := k.Write([]telegraf.Metric{
		metric1,
		invalidMetric,
		metric2,
	})
	assert.Nil(err, "Should not return error")

	// remaining valid metrics should still get written
	svc.AssertRequests(assert, []*kinesis.PutRecordsInput{
		{
			StreamName: &streamName,
			Records: []*kinesis.PutRecordsRequestEntry{
				{
					PartitionKey: &partitionKey,
					Data:         metric1Data,
				},
				{
					PartitionKey: &partitionKey,
					Data:         metric2Data,
				},
			},
		},
	})
}

type mockKinesisPutRecordsResponse struct {
	Output *kinesis.PutRecordsOutput
	Err    error
}

type mockKinesisPutRecords struct {
	kinesisiface.KinesisAPI

	requests  []*kinesis.PutRecordsInput
	responses []*mockKinesisPutRecordsResponse
}

func (m *mockKinesisPutRecords) SetupResponse(
	failedRecordCount int64,
	records []*kinesis.PutRecordsResultEntry,
) {

	m.responses = append(m.responses, &mockKinesisPutRecordsResponse{
		Err: nil,
		Output: &kinesis.PutRecordsOutput{
			FailedRecordCount: &failedRecordCount,
			Records:           records,
		},
	})
}

func (m *mockKinesisPutRecords) SetupSuccessfulResponse(
	recordCount int,
) {

	shard := "shardId-000000000003"
	failedRecordCount := zero

	records := []*kinesis.PutRecordsResultEntry{}
	for i := 0; i < recordCount; i++ {

		sequenceNumber := fmt.Sprintf("%d", i)

		records = append(records, &kinesis.PutRecordsResultEntry{
			SequenceNumber: &sequenceNumber,
			ShardId:        &shard,
		})
	}

	m.responses = append(m.responses, &mockKinesisPutRecordsResponse{
		Err: nil,
		Output: &kinesis.PutRecordsOutput{
			FailedRecordCount: &failedRecordCount,
			Records:           records,
		},
	})
}

func (m *mockKinesisPutRecords) SetupErrorResponse(err error) {

	m.responses = append(m.responses, &mockKinesisPutRecordsResponse{
		Err:    err,
		Output: nil,
	})
}

func (m *mockKinesisPutRecords) PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {

	reqNum := len(m.requests)
	if reqNum > len(m.responses) {
		return nil, fmt.Errorf("Response for request %+v not setup", reqNum)
	}

	m.requests = append(m.requests, input)

	resp := m.responses[reqNum]
	return resp.Output, resp.Err
}

func (m *mockKinesisPutRecords) AssertRequests(
	assert *assert.Assertions,
	expected []*kinesis.PutRecordsInput,
) {

	assert.Equal(
		len(expected),
		len(m.requests),
		fmt.Sprintf("Expected %v requests", len(expected)),
	)

	for i, expectedInput := range expected {
		actualInput := m.requests[i]

		assert.Equal(
			expectedInput.StreamName,
			actualInput.StreamName,
			fmt.Sprintf("Expected request %v to have correct StreamName", i),
		)

		assert.Equal(
			len(expectedInput.Records),
			len(actualInput.Records),
			fmt.Sprintf("Expected request %v to have %v Records", i, len(expectedInput.Records)),
		)

		for r, expectedRecord := range expectedInput.Records {
			actualRecord := actualInput.Records[r]

			assert.Equal(
				&expectedRecord.PartitionKey,
				&actualRecord.PartitionKey,
				fmt.Sprintf("Expected (request %v, record %v) to have correct PartitionKey", i, r),
			)

			assert.Equal(
				&expectedRecord.ExplicitHashKey,
				&actualRecord.ExplicitHashKey,
				fmt.Sprintf("Expected (request %v, record %v) to have correct ExplicitHashKey", i, r),
			)

			assert.Equal(
				expectedRecord.Data,
				actualRecord.Data,
				fmt.Sprintf("Expected (request %v, record %v) to have correct Data", i, r),
			)
		}
	}
}

func serializeMetric(
	serializer serializers.Serializer,
	metric telegraf.Metric,
	assert *assert.Assertions,
) []byte {

	data, err := serializer.Serialize(metric)
	assert.Nil(err, "Should serialize test metric")
	return data
}
