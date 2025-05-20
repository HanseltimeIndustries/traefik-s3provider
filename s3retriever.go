package s3provider

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gopkg.in/yaml.v3"
)

type Parser uint8

const (
	Unknown Parser = iota
	Json
	Yaml
)

var ValidParsersFromString = map[string]Parser{
	"json": Json,
	"yaml": Yaml,
}

func ParseParser(s string) (Parser, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	value, ok := ValidParsersFromString[s]
	if !ok {
		return Unknown, fmt.Errorf("%q is not a valid parser", s)
	}
	return Parser(value), nil
}

// represents data retrieved from config object in a bucket
type ConfigData struct {
	// The unmarshalled json struct
	json map[string]interface{}
	// the date at which it was last updated
	lastModifiedAt time.Time
}

type MinS3Api interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

type RetrieverConfig struct {
	// The bucket name
	Bucket string
	// the key of the object in that bucket name
	Key string
	// The way to parse the config object
	Parser Parser
}

type S3ObjectRetriever struct {
	RetrieverConfig
	// The s3 client configured
	client MinS3Api
	// Data that was previously retrieved
	data *ConfigData
}

type CredentialsGetter func(ctx context.Context) (aws.Credentials, error)

func (get CredentialsGetter) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return get(ctx)
}

// Creates a new object retriever that retrieves information for just one
// config file object
// Uses a cached s3 client with other retrievers
func NewS3ObjectRetriever(client MinS3Api, config RetrieverConfig) (*S3ObjectRetriever) {
	return &S3ObjectRetriever{
		client: client,
		RetrieverConfig: config,
	}
}

// Indicates that the last retrieved data is no longer in sync with what is in the bucket
func (retriever *S3ObjectRetriever) HasChanged(ctx context.Context) (bool, error) {
	if retriever.data == nil {
		return true, nil
	}

	resp, err := retriever.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(retriever.Bucket),
		Key:    aws.String(retriever.Key),
	})
	// TODO - do some error handling
	if err != nil {
		log.Printf("unable to get attributes for %s/%s: %v", retriever.Bucket, retriever.Key, err)
		return false, err
	}

	return resp.LastModified.After(retriever.data.lastModifiedAt), nil
}

// Replaces the data on this 
func (retriever *S3ObjectRetriever) Retrieve(ctx context.Context) error {
	// Get the object from S3
	output, err := retriever.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(retriever.Bucket),
		Key:    aws.String(retriever.Key),
	})
	if err != nil {
		log.Printf("failed to get object: %v", err)
		return err
	}
	defer output.Body.Close()

	// Serialize the object
	switch retriever.Parser {
	case Json:
		var jsonMap map[string]interface{}
		if err := json.NewDecoder(output.Body).Decode(&jsonMap); err != nil {
			log.Printf("failed to decode JSON for %s/%s: %v", retriever.Bucket, retriever.Key, err)
			return err
		}
		retriever.data = &ConfigData{
			json:           jsonMap,
			lastModifiedAt: *output.LastModified,
		}
	case Yaml:
		// var yamlMap map[string]interface{}
		var node yaml.Node
		if err := yaml.NewDecoder(output.Body).Decode(&node); err != nil {
			log.Printf("Failed to decode YAML for %s/%s: %v", retriever.Bucket, retriever.Key, err)
			return err
		}
		yamlMap, err := ensureNodesAreFloat(&node)
		if err != nil {
			log.Printf("Failed to convert decoded YAML to same types as decoded json for %s/%s: %v", retriever.Bucket, retriever.Key, err)
			return err
		}
		retriever.data = &ConfigData{
			json:           yamlMap.(map[string]interface{}),
			lastModifiedAt: *output.LastModified,
		}
	default:
		return fmt.Errorf("unknown parser for %s/%s: %v", retriever.Bucket, retriever.Key, err)
	}
	return nil
}

// make yaml and json interfaces type compatible to ensure merging
func ensureNodesAreFloat(node *yaml.Node) (interface{}, error) {
	switch node.Kind {
	case yaml.DocumentNode:
		return ensureNodesAreFloat(node.Content[0])
	case yaml.MappingNode:
		m := make(map[string]interface{})
		for i := 0; i < len(node.Content); i += 2 {
			key := node.Content[i].Value
			val, err := ensureNodesAreFloat(node.Content[i+1])
			if err != nil {
				return nil, err
			}
			m[key] = val
		}
		return m, nil
	case yaml.SequenceNode:
		s := make([]interface{}, len(node.Content))
		for i, n := range node.Content {
			el, err := ensureNodesAreFloat(n)
			if err != nil {
				return nil, err
			}
			s[i] = el
		}
		return s, nil
	case yaml.ScalarNode:
		switch node.Tag {
		case "!!int", "!!float":
			// Always parse as float64
			f, err := strconv.ParseFloat(node.Value, 64)
			if err != nil {
				return nil, err
			}
			return f, nil
		case "!!bool":
			b, err := strconv.ParseBool(node.Value)

			if err != nil {
				return nil, err
			}
			return b, nil
		}
		return node.Value, nil
	default:
		return nil, fmt.Errorf("unexpected yaml node kind to parse: %v", node.Kind)
	}
}