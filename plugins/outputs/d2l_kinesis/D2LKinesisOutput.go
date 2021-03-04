package d2lkinesis

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/influxdata/telegraf"
	internalaws "github.com/influxdata/telegraf/config/aws"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/serializers"
)

const defaultMaxRecordRetries = 10

// Limits set by AWS (https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html)
const awsMaxRecordsPerRequest = 500
const awsMaxRecordSize = 1048576  // 1 MiB
const awsMaxRequestSize = 5242880 // 5 MiB

type (
	d2lKinesisOutput struct {
		Region      string `toml:"region"`
		AccessKey   string `toml:"access_key"`
		SecretKey   string `toml:"secret_key"`
		RoleARN     string `toml:"role_arn"`
		Profile     string `toml:"profile"`
		Filename    string `toml:"shared_credential_file"`
		Token       string `toml:"token"`
		EndpointURL string `toml:"endpoint_url"`

		StreamName string `toml:"streamname"`

		MaxRecordRetries int `toml:"max_record_retries"`
		MaxRecordSize    int `toml:"max_record_size"`

		Log                  telegraf.Logger `toml:"-"`
		maxRecordsPerRequest int
		maxRequestSize       int
		serializer           serializers.Serializer
		svc                  kinesisiface.KinesisAPI
	}
)

var sampleConfig = `
  ## Amazon REGION of kinesis endpoint.
  region = "ap-southeast-2"

  ## Amazon Credentials
  ## Credentials are loaded in the following order
  ## 1) Assumed credentials via STS if role_arn is specified
  ## 2) explicit credentials from 'access_key' and 'secret_key'
  ## 3) shared profile from 'profile'
  ## 4) environment variables
  ## 5) shared credentials file
  ## 6) EC2 Instance Profile
  #access_key = ""
  #secret_key = ""
  #token = ""
  #role_arn = ""
  #profile = ""
  #shared_credential_file = ""

  ## Endpoint to make request against, the correct endpoint is automatically
  ## determined and this option should only be set if you wish to override the
  ## default.
  ##   ex: endpoint_url = "http://localhost:8000"
  # endpoint_url = ""

  ## Kinesis StreamName must exist prior to starting telegraf.
  streamname = "StreamName"

  ## Data format to output.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
  data_format = "influx"
`

// SampleConfig returns the default configuration of the Processor
func (k *d2lKinesisOutput) SampleConfig() string {
	return sampleConfig
}

// Description returns a one-sentence description on the Processor
func (k *d2lKinesisOutput) Description() string {
	return "Configuration for the D2L AWS Kinesis output."
}

// Connect to the Output; connect is only called once when the plugin starts
func (k *d2lKinesisOutput) Connect() error {

	k.maxRecordsPerRequest = awsMaxRecordsPerRequest

	credentialConfig := &internalaws.CredentialConfig{
		Region:      k.Region,
		AccessKey:   k.AccessKey,
		SecretKey:   k.SecretKey,
		RoleARN:     k.RoleARN,
		Profile:     k.Profile,
		Filename:    k.Filename,
		Token:       k.Token,
		EndpointURL: k.EndpointURL,
	}
	configProvider := credentialConfig.Credentials()
	svc := kinesis.New(configProvider)

	_, err := svc.DescribeStreamSummary(&kinesis.DescribeStreamSummaryInput{
		StreamName: aws.String(k.StreamName),
	})
	k.svc = svc
	return err
}

// Close any connections to the Output. Close is called once when the output
// is shutting down. Close will not be called until all writes have finished,
// and Write() will not be called once Close() has been, so locking is not
// necessary.
func (k *d2lKinesisOutput) Close() error {
	return nil
}

// SetSerializer sets the serializer function for the interface.
func (k *d2lKinesisOutput) SetSerializer(serializer serializers.Serializer) {
	k.serializer = serializer
}

// Write takes in group of points to be written to the Output
func (k *d2lKinesisOutput) Write(metrics []telegraf.Metric) error {

	if len(metrics) == 0 {
		return nil
	}

	generator, generatorErr := createGZipKinesisRecordGenerator(
		k.Log,
		k.MaxRecordSize,
		metrics,
		k.serializer,
	)
	if generatorErr != nil {
		return generatorErr
	}

	return k.writeRecords(generator)
}

func (k *d2lKinesisOutput) writeRecords(
	recordIterator kinesisRecordIterator,
) error {

	attempt := 0
	for {

		failedRecords, err := k.putRecordBatches(recordIterator)
		if err != nil {
			return err
		}

		failedCount := len(failedRecords)
		if failedCount == 0 {
			return nil
		}

		attempt++
		if attempt > k.MaxRecordRetries {

			k.Log.Errorf(
				"Unable to write %+v record(s) to Kinesis after %+v attempts",
				failedCount,
				attempt,
			)

			return nil
		}

		k.Log.Debugf(
			"Retrying %+v record(s)",
			failedCount,
		)
		recordIterator = createKinesisRecordSet(failedRecords)
	}
}

func (k *d2lKinesisOutput) putRecordBatches(
	recordIterator kinesisRecordIterator,
) ([]*kinesis.PutRecordsRequestEntry, error) {

	batchRecordCount := 0
	batchRequestSize := 0
	batch := []*kinesis.PutRecordsRequestEntry{}

	allFailedRecords := []*kinesis.PutRecordsRequestEntry{}

	for {
		record, recordErr := recordIterator.Next()
		if recordErr != nil {
			return nil, recordErr
		}
		if record == nil {
			break
		}

		// Partition keys are included in the limit calculation.
		// This is assuming partition keys are ASCII.
		recordRequestSize := len(record.Data) + len(*record.PartitionKey)
		if batchRequestSize+recordRequestSize > k.maxRequestSize {

			failedRecords := k.putRecords(batch)
			allFailedRecords = append(allFailedRecords, failedRecords...)

			batchRecordCount = 0
			batchRequestSize = 0
			batch = nil
		}

		batchRecordCount++
		batchRequestSize += recordRequestSize
		batch = append(batch, record)

		if batchRecordCount >= k.maxRecordsPerRequest {
			failedRecords := k.putRecords(batch)
			allFailedRecords = append(allFailedRecords, failedRecords...)

			batchRecordCount = 0
			batchRequestSize = 0
			batch = nil
		}
	}

	if batchRecordCount > 0 {
		failedRecords := k.putRecords(batch)
		allFailedRecords = append(allFailedRecords, failedRecords...)
	}

	return allFailedRecords, nil
}

func (k *d2lKinesisOutput) putRecords(
	records []*kinesis.PutRecordsRequestEntry,
) []*kinesis.PutRecordsRequestEntry {

	totalRecordCount := len(records)

	payload := &kinesis.PutRecordsInput{
		Records:    records,
		StreamName: aws.String(k.StreamName),
	}

	start := time.Now()
	resp, err := k.svc.PutRecords(payload)
	duration := time.Since(start)

	if err != nil {

		k.Log.Warnf(
			"Unable to write %+v records to Kinesis : %s",
			totalRecordCount,
			err.Error(),
		)
		return records
	}

	successfulRecordCount := int64(totalRecordCount) - *resp.FailedRecordCount

	k.Log.Debugf(
		"Wrote %+v of %+v record(s) to Kinesis in %s",
		successfulRecordCount,
		totalRecordCount,
		duration.String(),
	)

	var failedRecords []*kinesis.PutRecordsRequestEntry

	if *resp.FailedRecordCount > 0 {

		for i := 0; i < totalRecordCount; i++ {
			if resp.Records[i].ErrorCode != nil {
				failedRecords = append(failedRecords, records[i])
			}
		}
	}

	return failedRecords
}

func init() {
	outputs.Add("d2l_kinesis", func() telegraf.Output {
		return &d2lKinesisOutput{

			MaxRecordRetries: defaultMaxRecordRetries,
			MaxRecordSize:    awsMaxRecordSize,

			maxRecordsPerRequest: awsMaxRecordsPerRequest,
			maxRequestSize:       awsMaxRequestSize,
		}
	})
}

func calculateRequestOverheadSize(streamName string) int {
	return len("{\"Records\":[],\"StreamName\":\"\"}") + len(streamName)
}
