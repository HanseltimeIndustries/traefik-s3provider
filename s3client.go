package s3provider

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Do this once and continue to fail since it is something you would more than likely need to rebuild
// on the machine
func NewS3Client() (*s3.Client, error) {
	// Get the client defaults and then wrap the provider if we want to use refreshable credentials file
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	// TODO - do a refesh from file for credentials
	// wrap credentials and if "check file for refresh", then perform a stat check on the modified
	// if the file is modified perform a singleflight check

	// Create an S3 client
	client := s3.NewFromConfig(cfg)

	return client, nil
}