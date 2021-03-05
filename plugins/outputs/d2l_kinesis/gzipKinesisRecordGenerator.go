package d2lkinesis

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/base64"
	"math"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gofrs/uuid"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/serializers"
)

const gzipHeaderSize = 10
const gzipFooterSize = 8

func createGZipKinesisRecordGenerator(
	log telegraf.Logger,
	maxRecordSize int,
	metrics []telegraf.Metric,
	serializer serializers.Serializer,
) (kinesisRecordIterator, error) {

	buffer := bytes.NewBuffer([]byte{})

	writer, writerErr := gzip.NewWriterLevel(buffer, flate.BestCompression)
	if writerErr != nil {
		return nil, writerErr
	}

	generator := &gzipKinesisRecordGenerator{
		log:           log,
		maxRecordSize: maxRecordSize,
		metrics:       metrics,
		metricsCount:  len(metrics),
		serializer:    serializer,

		buffer: buffer,
		index:  0,
		writer: writer,
	}

	return generator, nil
}

type gzipKinesisRecordGenerator struct {
	kinesisRecordIterator

	log           telegraf.Logger
	maxRecordSize int
	metricsCount  int
	metrics       []telegraf.Metric
	serializer    serializers.Serializer

	buffer *bytes.Buffer
	index  int
	writer *gzip.Writer
}

func (g *gzipKinesisRecordGenerator) generatePartitionKey() string {
	id, err := uuid.NewV4()
	if err != nil {
		g.log.Errorf("Failed to generate partition key: %s", err.Error())
		return "default"
	}
	pk := base64.StdEncoding.EncodeToString(id.Bytes())
	return pk
}

func (g *gzipKinesisRecordGenerator) yieldRecord(
	metrics int,
) (*kinesisRecord, error) {

	closeErr := g.writer.Close()
	if closeErr != nil {
		return nil, closeErr
	}

	data := g.buffer.Bytes()
	partitionKey := g.generatePartitionKey()

	entry := kinesis.PutRecordsRequestEntry{
		Data:         data,
		PartitionKey: &partitionKey,
	}

	record := createKinesisRecord(&entry, metrics)

	return &record, nil
}

func (g *gzipKinesisRecordGenerator) Next() (*kinesisRecord, error) {

	startIndex := g.index
	if startIndex >= g.metricsCount {
		return nil, nil
	}

	g.buffer.Reset()
	g.writer.Reset(g.buffer)

	index := startIndex
	recordMetricCount := 0
	recordSize := gzipHeaderSize

	for ; index < g.metricsCount; index++ {
		metric := g.metrics[index]

		bytes, serializeErr := g.serializer.Serialize(metric)
		if serializeErr != nil {

			g.log.Errorf(
				"Failed to serialize metric: %s",
				serializeErr.Error(),
			)
			continue
		}

		bytesCount := len(bytes)
		maxCompressedBytes := bytesCount + 5*int((math.Floor(float64(bytesCount)/16383)+1))

		maxPotentialRecordSize := recordSize + maxCompressedBytes + 5 + gzipFooterSize
		if maxPotentialRecordSize > g.maxRecordSize {

			if recordMetricCount == 0 {
				g.log.Warnf(
					"Dropping excessively large '%s' metric",
					metric.Name(),
				)
				continue
			}

			g.index = index
			return g.yieldRecord(recordMetricCount)
		}

		_, writeErr := g.writer.Write(bytes)
		if writeErr != nil {
			return nil, writeErr
		}

		flushErr := g.writer.Flush()
		if flushErr != nil {
			return nil, flushErr
		}

		recordMetricCount++
		recordSize = g.buffer.Len()
	}

	if recordMetricCount > 0 {
		g.index = index + 1
		return g.yieldRecord(recordMetricCount)
	}

	return nil, nil
}
