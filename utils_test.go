package s3provider

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/mock"
)

type mockS3Client struct {
	mock.Mock
}

func newMockS3Client() *mockS3Client {
	return &mockS3Client{}
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	args := m.Called(ctx, params, optFns)

	resp := args.Get(0)

	if resp == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*s3.GetObjectOutput), args.Error(1)
}

func (m *mockS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	args := m.Called(ctx, params, optFns)

	resp := args.Get(0)

	if resp == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*s3.HeadObjectOutput), args.Error(1)
}