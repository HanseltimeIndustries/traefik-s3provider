package s3provider

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testBucket = "testbucket"
	testKey = "testkey"
	testJson = `{
    "value1": {
	  "arr": [
	    {
	      "inner": 1,
		  "value": 2
		},
		"stringValue"
	  ]
	},
	"thing": true,
	"num": [
	  13,
	  12
	]
}`
	testYaml = `
value1:
  arr:
  - inner: 1
    value: 2
  - stringValue
thing: true
num:
- 13
- 12`
	testBadJson = `{ "unterminated": }`
	testBadYaml = `service:
  another: 
    - value
	missing`
)

var (
	testJsonMap = map[string]interface{} {
		"value1": map[string]interface{} {
			"arr": []interface{} {
				map[string]interface{} {
					"inner": float64(1),
					"value": float64(2),
				},
				"stringValue",
			},
		},
		"thing": true,
		"num": []interface{}{float64(13), float64(12)},
	}
)

func TestHasChangedOnInitial(t *testing.T) {
	ctx := context.Background()
	mockClient := newMockS3Client()
	retriever := NewS3ObjectRetriever(mockClient, RetrieverConfig{
		Bucket: testBucket,
		Key: testKey,
		Parser: Yaml,
	})

	changed, err := retriever.HasChanged(ctx)
	require.Nil(t, err, "no has changed error")
	assert.True(t, changed, "initial retriever returns hasChanged true")

	// Ensure we didn't call the head object since we know we need to get some
	mockClient.AssertNotCalled(t, "HeadObject")
}

func TestHasChangedAPIError(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	mockClient := newMockS3Client()
	var emptyThird []func(*s3.Options) = nil
	mockClient.On("HeadObject", ctx, mock.MatchedBy(func(arg1 interface{}) bool {
		input, ok := arg1.(*s3.HeadObjectInput)
		if !ok {
			return false
		}
		return *input.Bucket == testBucket && *input.Key == testKey
	}), emptyThird).Return(&s3.HeadObjectOutput{
		LastModified: &now,
	}, errors.New("Oh no!"))
	retriever := NewS3ObjectRetriever(mockClient, RetrieverConfig{
		Bucket: testBucket,
		Key: testKey,
		Parser: Yaml,
	})

	retriever.data = &ConfigData{
		json: make(map[string]interface{}),
		lastModifiedAt: now,
	}

	changed, err := retriever.HasChanged(ctx)
	require.ErrorContains(t, err, "Oh no!")
	assert.False(t, changed, "has changed is false on error")
}

func TestHasChangedIfRetrieveOlder(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	mockClient := newMockS3Client()
	var emptyThird []func(*s3.Options) = nil
	mockClient.On("HeadObject", ctx, mock.Anything, mock.Anything).Return(&s3.HeadObjectOutput{
		LastModified: &now,
	}, nil)
	retriever := NewS3ObjectRetriever(mockClient, RetrieverConfig{
		Bucket: testBucket,
		Key: testKey,
		Parser: Yaml,
	})

	// Simulate older data
	retriever.data = &ConfigData{
		json: make(map[string]interface{}),
		lastModifiedAt: now.Add(time.Duration(-5) * time.Second),
	}

	changed, err := retriever.HasChanged(ctx)
	assert.Nil(t, err)
	require.True(t, changed, "has changed is false on error")
	mockClient.AssertCalled(t, "HeadObject", ctx, mock.MatchedBy(func(arg1 *s3.HeadObjectInput) bool {
		return *arg1.Bucket == testBucket && *arg1.Key == testKey
	}), emptyThird)
}

func TestHasChangedIfRetrieveSame(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	mockClient := newMockS3Client()
	var emptyThird []func(*s3.Options) = nil
	mockClient.On("HeadObject", ctx, mock.Anything, mock.Anything).Return(&s3.HeadObjectOutput{
		LastModified: &now,
	}, nil)
	retriever := NewS3ObjectRetriever(mockClient, RetrieverConfig{
		Bucket: testBucket,
		Key: testKey,
		Parser: Yaml,
	})

	retriever.data = &ConfigData{
		json: make(map[string]interface{}),
		lastModifiedAt: now,
	}

	changed, err := retriever.HasChanged(ctx)
	require.Nil(t, err)
	assert.False(t, changed, "has changed is false on equal")
	mockClient.AssertCalled(t, "HeadObject", ctx, mock.MatchedBy(func(arg1 *s3.HeadObjectInput) bool {
		return *arg1.Bucket == testBucket && *arg1.Key == testKey
	}), emptyThird)
}

func TestHasChangedIfRetrieveNewer(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	mockClient := newMockS3Client()
	var emptyThird []func(*s3.Options) = nil
	mockClient.On("HeadObject", ctx, mock.Anything, mock.Anything).Return(&s3.HeadObjectOutput{
		LastModified: &now,
	}, nil)
	retriever := NewS3ObjectRetriever(mockClient, RetrieverConfig{
		Bucket: testBucket,
		Key: testKey,
		Parser: Yaml,
	})

	retriever.data = &ConfigData{
		json: make(map[string]interface{}),
		lastModifiedAt: now.Add(time.Duration(5) * time.Second),
	}

	changed, err := retriever.HasChanged(ctx)
	require.Nil(t, err)
	assert.False(t, changed, "has changed is false on equal")
	mockClient.AssertCalled(t, "HeadObject", ctx, mock.MatchedBy(func(arg1 *s3.HeadObjectInput) bool {
		return *arg1.Bucket == testBucket && *arg1.Key == testKey
	}), emptyThird)
}

func TestRetrieveInitial(t *testing.T) {
	var tests = []struct {
		name string
		parser Parser
	} {
		{"yaml", Yaml},
		{"json", Json},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			ctx := context.Background()
			mockClient := newMockS3Client()
			var emptyThird []func(*s3.Options) = nil
			
			var raw string
			switch (tt.parser) {
			case Yaml:
				raw = testYaml
			case Json:
				raw = testJson
			default:
				t.Errorf("Unexpected parser for test %v", tt.parser)
				return
			}
			mockClient.On("GetObject", ctx, mock.Anything, mock.Anything).Return(&s3.GetObjectOutput{
				LastModified: &now,
				Body: io.NopCloser(bytes.NewReader([]byte(raw))),
			}, nil)
			retriever := NewS3ObjectRetriever(mockClient, RetrieverConfig{
				Bucket: testBucket,
				Key: testKey,
				Parser: tt.parser,
			})
		
			err := retriever.Retrieve(ctx)
			require.Nil(t, err)
			assert.Equal(t, &ConfigData{
				json: testJsonMap,
				lastModifiedAt: now,
			}, retriever.data)
			mockClient.AssertCalled(t, "GetObject", ctx, mock.MatchedBy(func(arg1 *s3.GetObjectInput) bool {
				return *arg1.Bucket == testBucket && *arg1.Key == testKey
			}), emptyThird)
		})
	}
}

func TestRetrieveOverwrite(t *testing.T) {
	var tests = []struct {
		name string
		parser Parser
	} {
		{"yaml", Yaml},
		{"json", Json},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			ctx := context.Background()
			mockClient := newMockS3Client()
			var emptyThird []func(*s3.Options) = nil
			
			var raw string
			switch (tt.parser) {
			case Yaml:
				raw = testYaml
			case Json:
				raw = testJson
			default:
				t.Errorf("Unexpected parser for test %v", tt.parser)
				return
			}
			mockClient.On("GetObject", ctx, mock.Anything, mock.Anything).Return(&s3.GetObjectOutput{
				LastModified: &now,
				Body: io.NopCloser(bytes.NewReader([]byte(raw))),
			}, nil)
			retriever := NewS3ObjectRetriever(mockClient, RetrieverConfig{
				Bucket: testBucket,
				Key: testKey,
				Parser: tt.parser,
			})

			retriever.data = &ConfigData{
				json: map[string]interface{} {
					"someValue": float64(22),
					"another": "string",
				},
			}
		
			err := retriever.Retrieve(ctx)
			require.Nil(t, err)
			assert.Equal(t, &ConfigData{
				json: testJsonMap,
				lastModifiedAt: now,
			}, retriever.data)
			mockClient.AssertCalled(t, "GetObject", ctx, mock.MatchedBy(func(arg1 *s3.GetObjectInput) bool {
				return *arg1.Bucket == testBucket && *arg1.Key == testKey
			}), emptyThird)
		})
	}
}

func TestRetrieveErrors(t *testing.T) {
	var tests = []struct {
		name string
		parser Parser
	} {
		{"yaml", Yaml},
		{"json", Json},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			ctx := context.Background()
			mockClient := newMockS3Client()
			var emptyThird []func(*s3.Options) = nil
			
			var raw, expectedErrorMatch string
			switch (tt.parser) {
			case Yaml:
				raw = testBadYaml
				expectedErrorMatch = "found a tab character that violates indentation"
			case Json:
				raw = testBadJson
				expectedErrorMatch = "invalid character '}'"
			default:
				t.Errorf("Unexpected parser for test %v", tt.parser)
				return
			}
			mockClient.On("GetObject", ctx, mock.Anything, mock.Anything).Return(&s3.GetObjectOutput{
				LastModified: &now,
				Body: io.NopCloser(bytes.NewReader([]byte(raw))),
			}, nil)
			retriever := NewS3ObjectRetriever(mockClient, RetrieverConfig{
				Bucket: testBucket,
				Key: testKey,
				Parser: tt.parser,
			})
		
		     

			err := retriever.Retrieve(ctx)
			require.ErrorContains(t, err, expectedErrorMatch)
			assert.Nil(t, retriever.data)
			mockClient.AssertCalled(t, "GetObject", ctx, mock.MatchedBy(func(arg1 *s3.GetObjectInput) bool {
				return *arg1.Bucket == testBucket && *arg1.Key == testKey
			}), emptyThird)
		})
	}
}