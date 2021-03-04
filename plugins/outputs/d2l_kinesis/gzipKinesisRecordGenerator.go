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

const gzipHeaderFooterSize = 18 // gzip header (10) + footer size (8)

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

func (g *gzipKinesisRecordGenerator) getPartitionKey() string {
	id, err := uuid.NewV4()
	if err != nil {
		g.log.Errorf("Failed to generate partition key: %s", err.Error())
		return "default"
	}
	pk := base64.StdEncoding.EncodeToString(id.Bytes())
	return pk
}

func (g *gzipKinesisRecordGenerator) emitRecord() (*kinesis.PutRecordsRequestEntry, error) {

	closeErr := g.writer.Close()
	if closeErr != nil {
		return nil, closeErr
	}

	data := g.buffer.Bytes()
	partitionKey := g.getPartitionKey()

	record := kinesis.PutRecordsRequestEntry{
		Data:         data,
		PartitionKey: &partitionKey,
	}

	return &record, nil
}

func (g *gzipKinesisRecordGenerator) Next() (*kinesis.PutRecordsRequestEntry, error) {

	startIndex := g.index
	if startIndex >= g.metricsCount {
		return nil, nil
	}

	g.buffer.Reset()
	g.writer.Reset(g.buffer)

	index := startIndex
	recordSize := gzipHeaderFooterSize

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

		if recordSize+maxCompressedBytes > g.maxRecordSize {

			if index == startIndex {
				g.log.Warnf(
					"Dropping excessively large '%s' metric",
					metric.Name(),
				)
				continue
			}

			g.index = index
			return g.emitRecord()
		}

		bytesWritten, writeErr := g.writer.Write(bytes)
		if writeErr != nil {
			return nil, writeErr
		}
		recordSize += bytesWritten
	}

	g.index = index + 1
	return g.emitRecord()
}
