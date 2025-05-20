package s3provider

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	json1 = `{
	"tls": {
	  "certificates": [
		{
	      "certFile": "certpath",
		  "keyFile": "keypath"
		},
		{
		  "certFile": "certpath2",
		  "keyFile": "keypath2"
		}
	 ],
	 "additional": "somevalue"
	}	
}`
json2 = `{
	"tls": {
	  "certificates": [
		{
		  "certFile": "certpath2",
		  "keyFile": "keypath2"
		},
		{
	      "certFile": "certpath",
		  "keyFile": "keypath"
		}
	 ],
	 "newAdditional": "diffvalue"
	}	
}`
	yaml1 = `
tls:
  certificates:
    - certFile: /path/to/domain.cert
      keyFile: /path/to/domain.key
    - certFile: /path/to/other-domain.cert
      keyFile: /path/to/other-domain.key`
)

var (
	json1AndYaml1 = map[string]interface{} {
		"tls": map[string]interface{} {
			"certificates": []map[string]interface{} {
				{
					"certFile": "certpath",
					"keyFile": "keypath",
				},
				{
					"certFile": "certpath2",
					"keyFile": "keypath2",
				},
				{
					"certFile": "/path/to/domain.cert",
					"keyFile": "/path/to/domain.key",
				},
				{
					"certFile": "/path/to/other-domain.cert",
					"keyFile": "/path/to/other-domain.key",
				},
			},
			"additional": "somevalue",
		},
	}
	json2AndYaml1 = map[string]interface{} {
		"tls": map[string]interface{} {
			"certificates": []map[string]interface{} {
				{
					"certFile": "certpath2",
					"keyFile": "keypath2",
				},
				{
					"certFile": "certpath",
					"keyFile": "keypath",
				},
				{
					"certFile": "/path/to/domain.cert",
					"keyFile": "/path/to/domain.key",
				},
				{
					"certFile": "/path/to/other-domain.cert",
					"keyFile": "/path/to/other-domain.key",
				},
			},
			"newAdditional": "diffvalue",
		},
	}
)

func TestNewPollIntervalValidation(t *testing.T) {
	var config Config
	json.Unmarshal([]byte(`{"pollInterval": "0s"}`), &config)

	provider, err := New(context.Background(), &config, "test")
	assert.ErrorContains(t, err, "poll interval must be greater than 0")
	assert.Nil(t, provider)
}

func TestNewPollIntervalValidationSyntax(t *testing.T) {
	var config Config
	json.Unmarshal([]byte(`{"pollInterval": "5sfo"}`), &config)

	provider, err := New(context.Background(), &config, "test")
	assert.ErrorContains(t, err, "unknown unit")
	assert.Nil(t, provider)
}

func TestNewObjectsValidationSyntax(t *testing.T) {
	var config Config
	json.Unmarshal([]byte(`{"pollInterval": "5s", "objects": []}`), &config)

	provider, err := New(context.Background(), &config, "test")
	assert.ErrorContains(t, err, "objects must be non-empty to use s3 provider")
	assert.Nil(t, provider)
}

func TestNewObjectsEmptyKeyValidationSyntax(t *testing.T) {
	var config Config
	json.Unmarshal([]byte(`{"pollInterval": "5s", "objects": [
		{
		"key": "",
		"bucket": "somebucket"
		}
	]}`), &config)

	provider, err := New(context.Background(), &config, "test")
	assert.ErrorContains(t, err, "cannot have empty key")
	assert.Nil(t, provider)
}

func TestNewObjectsEmptyBucketValidationSyntax(t *testing.T) {
	var config Config
	json.Unmarshal([]byte(`{"pollInterval": "5s", "objects": [
		{
		"key": "huh",
		"bucket": ""
		}
	]}`), &config)

	provider, err := New(context.Background(), &config, "test")
	assert.ErrorContains(t, err, "cannot have empty bucket")
	assert.Nil(t, provider)
}

func TestNewObjectsInferredParser(t *testing.T) {
	var config Config
	json.Unmarshal([]byte(`{"pollInterval": "5s", "objects": [
		{
			"key": "huh.json",
			"bucket": "someBucket"
		},
		{
			"key": "f.yml",
			"bucket": "someBucket"
		},
		{
			"key": "f.yaml",
			"bucket": "someBucket"
		}
	]}`), &config)

	provider, err := New(context.Background(), &config, "test")
	require.Nil(t, err)
	require.Len(t, provider.retrievers, 3)
	require.Equal(t, Json, provider.retrievers[0].RetrieverConfig.Parser)
	require.Equal(t, Yaml, provider.retrievers[1].RetrieverConfig.Parser)
	require.Equal(t, Yaml, provider.retrievers[2].RetrieverConfig.Parser)
}

func TestNewObjectsInferredParserValidationSyntax(t *testing.T) {
	var config Config
	json.Unmarshal([]byte(`{"pollInterval": "5s", "objects": [
		{
			"key": "huh.json",
			"bucket": "someBucket"
		},
		{
			"key": "f.yml",
			"bucket": "someBucket"
		},
		{
			"key": "f2.txt",
			"bucket": "someBucket"
		},
		{
			"key": "f.yaml",
			"bucket": "someBucket"
		}
	]}`), &config)

	_, err := New(context.Background(), &config, "test")
	require.ErrorContains(t, err, "cannot infer parser for key f2.txt")
}

func TestMergedFiles(t *testing.T) {
	var config Config
	json.Unmarshal([]byte(`{"pollInterval": "1s", "objects": [
		{
			"key": "huh.json",
			"bucket": "someBucket"
		},
		{
			"key": "f.yml",
			"bucket": "someBucket"
		}
	]}`), &config)

	ctx := context.Background()
	provider, err := New(ctx, &config, "test")
	require.Nil(t, err)
	// mock the retrievers
	require.Len(t, provider.retrievers, 2)

	t.Cleanup(func() {
		err = provider.Stop()
		if err != nil {
			t.Fatal(err)
		}
	})

	// create mock retrievers
	s3Client := newMockS3Client()
	provider.retrievers[0].client = s3Client
	provider.retrievers[1].client = s3Client
	
	now := time.Now()
	s3Client.On("HeadObject", ctx, mock.Anything, mock.Anything).Return(&s3.HeadObjectOutput{
		LastModified: &now,
	})
	matchJson := mock.MatchedBy(func (arg *s3.GetObjectInput) (bool) {
		return *arg.Bucket == "someBucket" && *arg.Key == "huh.json"
	})
	matchYaml := mock.MatchedBy(func (arg *s3.GetObjectInput) (bool) {
		return *arg.Bucket == "someBucket" && *arg.Key == "f.yml"
	})
	s3Client.On("GetObject", mock.Anything, matchYaml, mock.Anything).Return(&s3.GetObjectOutput{
		LastModified: &now,
		Body: io.NopCloser(bytes.NewReader([]byte(yaml1))),
	}, nil)
	s3Client.On("GetObject", mock.Anything, matchJson, mock.Anything).Return(&s3.GetObjectOutput{
		LastModified: &now,
		Body: io.NopCloser(bytes.NewReader([]byte(json1))),
	}, nil)

	provider.Init()

	if err != nil {
		t.Fatal(err)
	}

	cfgChan := make(chan json.Marshaler)

	err = provider.Provide(cfgChan)
	if err != nil {
		t.Fatal(err)
	}

	data := <-cfgChan

	expBytes, _ := json.Marshal(json1AndYaml1)
	received, err := data.MarshalJSON()

	require.NoError(t, err)
	assert.Equal(t, string(expBytes[:]), string(received[:]))

	s3Client.AssertCalled(t, "GetObject", mock.Anything, matchJson, mock.Anything)
	s3Client.AssertCalled(t, "GetObject", mock.Anything, matchYaml, mock.Anything)
	s3Client.AssertNumberOfCalls(t, "GetObject", 2)
}

func TestMergedFilesOverwrite(t *testing.T) {
	var config Config
	json.Unmarshal([]byte(`{"pollInterval": "1s", "objects": [
		{
			"key": "huh.json",
			"bucket": "someBucket"
		},
		{
			"key": "f.yml",
			"bucket": "someBucket"
		}
	]}`), &config)

	ctx := context.Background()
	provider, err := New(ctx, &config, "test")
	require.Nil(t, err)
	// mock the retrievers
	require.Len(t, provider.retrievers, 2)

	t.Cleanup(func() {
		err = provider.Stop()
		if err != nil {
			t.Fatal(err)
		}
	})

	// create mock retrievers
	s3Client := newMockS3Client()
	provider.retrievers[0].client = s3Client
	provider.retrievers[1].client = s3Client
	
	now := time.Now()
	next := now.Add(time.Duration(5) * time.Second)
	matchJsonHead := mock.MatchedBy(func (arg *s3.HeadObjectInput) (bool) {
		return *arg.Bucket == "someBucket" && *arg.Key == "huh.json"
	})
	matchYamlHead := mock.MatchedBy(func (arg *s3.HeadObjectInput) (bool) {
		return *arg.Bucket == "someBucket" && *arg.Key == "f.yml"
	})
	s3Client.On("HeadObject", mock.Anything, matchJsonHead, mock.Anything).Return(&s3.HeadObjectOutput{
		LastModified: &now,
	}, nil).Once()
	s3Client.On("HeadObject", mock.Anything, matchJsonHead, mock.Anything).Return(&s3.HeadObjectOutput{
		LastModified: &next,
	}, nil)
	s3Client.On("HeadObject", mock.Anything, matchYamlHead, mock.Anything).Return(&s3.HeadObjectOutput{
		LastModified: &now,
	}, nil)
	matchJson := mock.MatchedBy(func (arg *s3.GetObjectInput) (bool) {
		return *arg.Bucket == "someBucket" && *arg.Key == "huh.json"
	})
	matchYaml := mock.MatchedBy(func (arg *s3.GetObjectInput) (bool) {
		return *arg.Bucket == "someBucket" && *arg.Key == "f.yml"
	})
	s3Client.On("GetObject", mock.Anything, matchYaml, mock.Anything).Return(&s3.GetObjectOutput{
		LastModified: &now,
		Body: io.NopCloser(bytes.NewReader([]byte(yaml1))),
	}, nil)
	s3Client.On("GetObject", mock.Anything, matchJson, mock.Anything).Return(&s3.GetObjectOutput{
		LastModified: &now,
		Body: io.NopCloser(bytes.NewReader([]byte(json1))),
	}, nil).Once()
	s3Client.On("GetObject", mock.Anything, matchJson, mock.Anything).Return(&s3.GetObjectOutput{
		LastModified: &now,
		Body: io.NopCloser(bytes.NewReader([]byte(json2))),
	}, nil).Once()

	provider.Init()

	if err != nil {
		t.Fatal(err)
	}

	cfgChan := make(chan json.Marshaler)

	err = provider.Provide(cfgChan)
	if err != nil {
		t.Fatal(err)
	}
	// Get once
	<-cfgChan
	println("Found data")
	secondData := <- cfgChan

	expBytes, _ := json.Marshal(json2AndYaml1)
	received, err := secondData.MarshalJSON()

	require.NoError(t, err)
	assert.Equal(t, string(expBytes[:]), string(received[:]))
}

func TestMergedFilesNoChange(t *testing.T) {
	var config Config
	json.Unmarshal([]byte(`{"pollInterval": "1s", "objects": [
		{
			"key": "huh.json",
			"bucket": "someBucket"
		},
		{
			"key": "f.yml",
			"bucket": "someBucket"
		}
	]}`), &config)

	ctx := context.Background()
	provider, err := New(ctx, &config, "test")
	require.Nil(t, err)
	// mock the retrievers
	require.Len(t, provider.retrievers, 2)

	t.Cleanup(func() {
		err = provider.Stop()
		if err != nil {
			t.Fatal(err)
		}
	})

	// create mock retrievers
	s3Client := newMockS3Client()
	provider.retrievers[0].client = s3Client
	provider.retrievers[1].client = s3Client
	
	now := time.Now()
	matchJsonHead := mock.MatchedBy(func (arg *s3.HeadObjectInput) (bool) {
		return *arg.Bucket == "someBucket" && *arg.Key == "huh.json"
	})
	matchYamlHead := mock.MatchedBy(func (arg *s3.HeadObjectInput) (bool) {
		return *arg.Bucket == "someBucket" && *arg.Key == "f.yml"
	})
	s3Client.On("HeadObject", mock.Anything, matchJsonHead, mock.Anything).Return(&s3.HeadObjectOutput{
		LastModified: &now,
	}, nil)
	s3Client.On("HeadObject", mock.Anything, matchYamlHead, mock.Anything).Return(&s3.HeadObjectOutput{
		LastModified: &now,
	}, nil)
	matchJson := mock.MatchedBy(func (arg *s3.GetObjectInput) (bool) {
		return *arg.Bucket == "someBucket" && *arg.Key == "huh.json"
	})
	matchYaml := mock.MatchedBy(func (arg *s3.GetObjectInput) (bool) {
		return *arg.Bucket == "someBucket" && *arg.Key == "f.yml"
	})
	s3Client.On("GetObject", mock.Anything, matchYaml, mock.Anything).Return(&s3.GetObjectOutput{
		LastModified: &now,
		Body: io.NopCloser(bytes.NewReader([]byte(yaml1))),
	}, nil)
	s3Client.On("GetObject", mock.Anything, matchJson, mock.Anything).Return(&s3.GetObjectOutput{
		LastModified: &now,
		Body: io.NopCloser(bytes.NewReader([]byte(json1))),
	}, nil)

	provider.retrievers[0].data = &ConfigData{
		lastModifiedAt: now,
		json: make(map[string]interface{}),
	}
	provider.retrievers[1].data = &ConfigData{
		lastModifiedAt: now,
		json: make(map[string]interface{}),
	}

	provider.Init()

	if err != nil {
		t.Fatal(err)
	}

	cfgChan := make(chan json.Marshaler)

	err = provider.Provide(cfgChan)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case val := <-cfgChan:
		require.FailNow(t, "channels ended up returning data: %v", val)
	case <-time.After(3 * time.Second):
		provider.Stop()
		close(cfgChan)
	}
}
