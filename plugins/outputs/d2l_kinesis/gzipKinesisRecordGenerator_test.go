package d2lkinesis

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/serializers"
	"github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var influxSerializer serializers.Serializer = influx.NewSerializer()

const testPartitionKey string = "abc"

func testPartitionKeyProvider() string {
	return testPartitionKey
}

func TestCreateGZipKinesisRecordGenerator(t *testing.T) {
	assert := assert.New(t)

	generator, err := createGZipKinesisRecordGenerator(
		testutil.Logger{},
		256,
		testPartitionKeyProvider,
		influxSerializer,
	)

	assert.NoError(err)
	assert.NotNil(generator)
}

func Test_GZipKinesisRecordGenerator_ZeroRecords(t *testing.T) {
	assert := assert.New(t)

	generator := createTestGZipKinesisRecordGenerator(t, 1024)
	generator.Reset([]telegraf.Metric{})

	record, err := generator.Next()
	assert.NoError(err, "Next should not error")
	assert.Nil(record)
}

func Test_GZipKinesisRecordGenerator_SingleRecord(t *testing.T) {
	assert := assert.New(t)

	metric, metricData := createTestMetric(t, "test", influxSerializer)

	generator := createTestGZipKinesisRecordGenerator(t, 1024)
	generator.Reset([]telegraf.Metric{metric})

	record1, err := generator.Next()
	assert.NoError(err, "Next should not error")
	assert.NotNil(record1)

	record2, err := generator.Next()
	assert.NoError(err, "Next should not error")
	assert.Nil(record2)

	assertKinesisRecord(
		assert,
		createTestKinesisRecord(t, 1, metricData),
		record1,
	)
}

func createTestGZipKinesisRecordGenerator(
	t *testing.T,
	maxRecordSize int,
) kinesisRecordGenerator {

	generator, err := createGZipKinesisRecordGenerator(
		testutil.Logger{},
		256,
		testPartitionKeyProvider,
		influxSerializer,
	)
	require.NoError(t, err)

	return generator
}

func createTestMetric(
	t *testing.T,
	name string,
	serializer serializers.Serializer,
) (telegraf.Metric, []byte) {

	metric := testutil.TestMetric(1, name)

	data, err := serializer.Serialize(metric)
	require.NoError(t, err)

	return metric, data
}

func assertKinesisRecord(
	assert *assert.Assertions,
	expected *kinesisRecord,
	actual *kinesisRecord,
) {

	assert.Equal(
		base64.StdEncoding.EncodeToString(expected.Entry.Data),
		base64.StdEncoding.EncodeToString(actual.Entry.Data),
		"Entry.Data should be as expected",
	)

	assert.Nil(
		expected.Entry.ExplicitHashKey,
		"Entry.ExplicitHashKey should not be expected",
	)
	assert.Nil(
		expected.Entry.ExplicitHashKey,
		"Entry.ExplicitHashKey should not be assigned",
	)

	assert.Equal(
		*expected.Entry.PartitionKey,
		*actual.Entry.PartitionKey,
		"Entry.PartitionKey should be as expected",
	)

	assert.Equal(
		expected.Metrics,
		actual.Metrics,
		"Metrics should be as expected",
	)

	assert.Equal(
		expected.RequestSize,
		actual.RequestSize,
		"RequestSize should be as expected",
	)
}

func createTestKinesisRecord(
	t *testing.T,
	metrics int,
	uncompressedData []byte,
) *kinesisRecord {

	partitionKey := testPartitionKey

	entry := &kinesis.PutRecordsRequestEntry{
		Data:         gzipData(t, uncompressedData),
		PartitionKey: &partitionKey,
	}

	return createKinesisRecord(entry, metrics)
}

func gzipData(
	t *testing.T,
	data []byte,
) []byte {

	buffer := bytes.NewBuffer([]byte{})

	writer, writerErr := gzip.NewWriterLevel(buffer, flate.BestCompression)
	require.NoError(t, writerErr, "Should create writer")

	count, writeErr := writer.Write(data)
	require.NoError(t, writeErr, "Should write data")
	require.Equal(t, len(data), count, "Should write all data")

	closeErr := writer.Close()
	require.NoError(t, closeErr, "Should close gzip writer")

	return buffer.Bytes()
}
