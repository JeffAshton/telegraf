package d2lkinesis

import (
	"bytes"
	"compress/flate"
	"compress/gzip"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gofrs/uuid"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/serializers"
)

const maxEmitRecordThreshold = 750000

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

	emitRecordThreshold := maxRecordSize
	if emitRecordThreshold > maxEmitRecordThreshold {
		emitRecordThreshold = maxEmitRecordThreshold
	}

	generator := &gzipKinesisRecordGenerator{
		log:                 log,
		emitRecordThreshold: emitRecordThreshold,
		metrics:             metrics,
		metricsCount:        len(metrics),
		serializer:          serializer,

		buffer: buffer,
		index:  0,
		writer: writer,
	}

	return generator, nil
}

type gzipKinesisRecordGenerator struct {
	kinesisRecordIterator

	log                 telegraf.Logger
	emitRecordThreshold int
	metricsCount        int
	metrics             []telegraf.Metric
	serializer          serializers.Serializer

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
	return id.String()
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

	index := g.index
	if index >= g.metricsCount {
		return nil, nil
	}

	g.buffer.Reset()
	g.writer.Reset(g.buffer)

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

		_, writeErr := g.writer.Write(bytes)
		if writeErr != nil {
			return nil, writeErr
		}

		flushErr := g.writer.Flush()
		if flushErr != nil {
			return nil, flushErr
		}

		if g.buffer.Len() > g.emitRecordThreshold {
			return g.emitRecord()
		}
	}

	return g.emitRecord()
}
