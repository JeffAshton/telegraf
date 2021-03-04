package d2lkinesis

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/influxdata/telegraf"
	internalaws "github.com/influxdata/telegraf/config/aws"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/serializers"
)

type (
	D2LKinesisOutput struct {
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
		maxRecordSize        int
		maxRecordsPerRequest int
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
func (k *D2LKinesisOutput) SampleConfig() string {
	return sampleConfig
}

// Description returns a one-sentence description on the Processor
func (k *D2LKinesisOutput) Description() string {
	return "Configuration for the D2L AWS Kinesis output."
}

// Connect to the Output; connect is only called once when the plugin starts
func (k *D2LKinesisOutput) Connect() error {

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
func (k *D2LKinesisOutput) Close() error {
	return nil
}

// SetSerializer sets the serializer function for the interface.
func (k *D2LKinesisOutput) SetSerializer(serializer serializers.Serializer) {
	k.serializer = serializer
}

// Write takes in group of points to be written to the Output
func (k *D2LKinesisOutput) Write(metrics []telegraf.Metric) error {

	var recordIterator kinesisRecordIterator

	metricsCount := len(metrics)
	if metricsCount == 0 {
		return nil
	}

	recordIterator, generatorErr := createGZipKinesisRecordGenerator(
		k.Log,
		k.MaxRecordSize,
		metrics,
		k.serializer,
	)
	if generatorErr != nil {
		return generatorErr
	}

	var failedRecords []*kinesis.PutRecordsRequestEntry

	for i := 0; i < k.MaxRecordRetries; i++ {

		failedRecords, err := k.putRecordBatches(recordIterator)
		if err != nil {
			return err
		}
		if len(failedRecords) > 0 {
			return nil
		}

		recordIterator = createKinesisRecordSet(failedRecords)
	}

	failedCount := len(failedRecords)
	if failedCount > 0 {

		k.Log.Warnf(
			"Unable to write %+v of %+v record(s) to Kinesis",
			failedCount,
			metricsCount,
		)
	}

	return nil
}

func (k *D2LKinesisOutput) putRecordBatches(
	recordIterator kinesisRecordIterator,
) ([]*kinesis.PutRecordsRequestEntry, error) {

	batchRecordCount := 0
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

		batchRecordCount++
		batch = append(batch, record)

		if batchRecordCount >= k.maxRecordsPerRequest {
			failedRecords := k.putRecords(batch)
			allFailedRecords = append(allFailedRecords, failedRecords...)

			batchRecordCount = 0
			batch = nil
		}
	}

	if batchRecordCount > 0 {
		failedRecords := k.putRecords(batch)
		allFailedRecords = append(allFailedRecords, failedRecords...)
	}

	return allFailedRecords, nil
}

func (k *D2LKinesisOutput) putRecords(
	records []*kinesis.PutRecordsRequestEntry,
) []*kinesis.PutRecordsRequestEntry {

	recordsCount := len(records)

	payload := &kinesis.PutRecordsInput{
		Records:    records,
		StreamName: aws.String(k.StreamName),
	}

	resp, err := k.svc.PutRecords(payload)
	if err != nil {

		k.Log.Warnf(
			"Unable to write %+v records to Kinesis : %s",
			recordsCount,
			err.Error(),
		)
		return records
	}

	var failedRecords []*kinesis.PutRecordsRequestEntry

	failed := *resp.FailedRecordCount
	if failed > 0 {

		k.Log.Warnf(
			"Unable to write %+v of %+v record(s) to Kinesis",
			failed,
			recordsCount,
		)

		for i := 0; i < recordsCount; i++ {
			if resp.Records[i].ErrorCode != nil {
				failedRecords = append(failedRecords, records[i])
			}
		}
	}

	return failedRecords
}

func init() {
	outputs.Add("d2l_kinesis", func() telegraf.Output {
		return &D2LKinesisOutput{

			// Limit set by AWS (https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html)
			maxRecordsPerRequest: 500,
		}
	})
}
