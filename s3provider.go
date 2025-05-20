// Allows for the download of traefik dynamic configuration via an S3 compliant object store
package s3provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"dario.cat/mergo"
)

type ObjectReference struct {
	// The bucket key that this file is under
	Key string `json:"key"`
	// The bucket to look up against
	Bucket string `json:"bucket"`
	// If we cannot auto-infer the parser from the extension, you can explicitly supply this
	Parser Parser `json:"parser,omitempty"`
}

// Config the plugin configuration.
type Config struct {
	// A Golang duration string for the interval at which we check for changes
	PollInterval string `json:"pollInterval,omitempty"`
	// A list of s3 bucket objects
	Objects []ObjectReference `json:"objects"`
}

// Simple trusted marshaler that returns bytes
type BytesProvider func() ([]byte, error)

func (bytesProvider BytesProvider) MarshalJSON() ([]byte, error) {
	return bytesProvider()
}

// CreateConfig creates the default plugin configuration.
func CreateConfig() *Config {
	return &Config{
		// The rate at which we will check the s3 objects to see if any have changed
		PollInterval: "300", // 300 * time.Second
	}
}

// Provider a simple provider plugin.
type Provider struct {
	name         string
	pollInterval time.Duration
	// 1 retriever per bucket object
	retrievers []*S3ObjectRetriever

	// The context cancel function for stopping our provider's goroutines
	cancel func()
}

// New creates a new Provider plugin.
func New(ctx context.Context, config *Config, name string) (*Provider, error) {
	pi, err := time.ParseDuration(config.PollInterval)
	if err != nil {
		return nil, err
	}

	if pi <= 0 {
		return nil, errors.New("poll interval must be greater than 0")
	}

	if len(config.Objects) == 0 {
		return nil, errors.New("objects must be non-empty to use s3 provider")
	}

	s3Client, err := NewS3Client()
	if err != nil {
		return nil, err
	}

	numObjs := len(config.Objects)
	retrievers := make([]*S3ObjectRetriever, numObjs)
	for idx, obj := range config.Objects {
		// index is the index where we are
		// element is the element from someSlice for where we are
		if len(obj.Key) == 0 {
			return nil, fmt.Errorf("object[%d] cannot have empty key %v", idx, obj)
		}
		if len(obj.Bucket) == 0 {
			return nil, fmt.Errorf("object[%d] cannot have empty bucket name %v", idx, obj)
		}
		if obj.Parser == Unknown {
			switch filepath.Ext(obj.Key) {
			case ".yaml":
				obj.Parser = Yaml
			case ".yml":
				obj.Parser = Yaml
			case ".json":
				obj.Parser = Json
			default:
				return nil, fmt.Errorf("object[%d] cannot infer parser for key %s. Must have a known extension or explicitly set parser", idx, obj.Key)
			}
		}

		// Create the object retriever that we can re-apply
		retrievers[idx] = NewS3ObjectRetriever(s3Client, RetrieverConfig{
			Bucket: obj.Bucket,
			Key: obj.Key,
			Parser: obj.Parser,
		})
	}

	return &Provider{
		name:         name,
		pollInterval: pi,
		retrievers:   retrievers,
	}, nil
}

// Init the provider.
func (p *Provider) Init() error {
	return nil
}

// Provide creates and send dynamic configuration.
func (p *Provider) Provide(cfgChan chan<- json.Marshaler) error {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Print(err)
			}
		}()

		p.pollConfiguration(ctx, cfgChan)
	}()

	return nil
}

func (p *Provider) pollConfiguration(ctx context.Context, cfgChan chan<- json.Marshaler) {
	// Run immediately
	p.provideConfiguration(ctx, cfgChan)
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check on intervals
			p.provideConfiguration(ctx, cfgChan)
		case <-ctx.Done():
			return
		}
	}
}

func (p *Provider) provideConfiguration(ctx context.Context, cfgChan chan<- json.Marshaler) {
	data, err := p.getConfiguration(ctx)
	if err != nil || data != nil {
		cfgChan <- BytesProvider(func() ([]byte, error) {
			return data, err
		})
	}
}

// Stop to stop the provider and the related go routines.
func (p *Provider) Stop() error {
	p.cancel()
	return nil
}

func (p *Provider) getConfiguration(ctx context.Context) ([]byte, error) {
	var err error
	// Check to see if the file has changed
	hasChanged := false
	for _, retriever := range p.retrievers {
		var changed bool
		changed, err = retriever.HasChanged(ctx)
		if err != nil {
			break
		}
		if changed {
			err = retriever.Retrieve(ctx)
			if err != nil {
				break
			}
			hasChanged = true
		}
	}

	// If we can't get a config, we pass it as a marshalling failure
	if err != nil {
		return make([]byte, 0), err
	}

	if hasChanged {
		var composite map[string]interface{} = make(map[string]interface{})
		// Remerge the json to ensure there's appropriate overriding
		for _, retriever := range p.retrievers {
			err = mergo.Merge(&composite, retriever.data.json, mergo.WithAppendSlice)
			if err != nil {
				break
			}
		}

		// Pass the error as a marshalling error to traefik
		if err != nil {
			return make([]byte, 0), err
		} else {
			return json.Marshal(composite)
		}
	}

	return nil, nil
}
