package elasticsearch

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type connectConfig struct {
	username string
	password string
	debug    bool
}

type connectOption func(*connectConfig)

// WithUsername sets the username for the elasticsearch connection.
func WithUsername(username string) connectOption {
	return func(c *connectConfig) {
		c.username = username
	}
}

// WithPassword sets the password for the elasticsearch connection.
func WithPassword(password string) connectOption {
	return func(c *connectConfig) {
		c.password = password
	}
}

// WithDebug sets the debug mode for the elasticsearch connection.
func WithDebug(debug bool) connectOption {
	return func(c *connectConfig) {
		c.debug = debug
	}
}

// Connect to Elasticsearch.
func Connect(ctx context.Context, elasticsearchURL string, options ...connectOption) (*elasticsearch.Client, error) {
	config := &connectConfig{}
	for _, option := range options {
		option(config)
	}

	cfg := elasticsearch.Config{
		Addresses:     []string{elasticsearchURL},
		Username:      config.username,
		Password:      config.password,
		RetryOnStatus: []int{429, 502, 503, 504},
		MaxRetries:    5,
		RetryBackoff: func(i int) time.Duration {
			return time.Duration(i) * 100 * time.Millisecond
		},
		// CompressRequestBody:  true,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // accept self-signed certs
			},
		},
	}
	if config.debug {
		cfg.EnableDebugLogger = true
		cfg.Logger = &elastictransport.TextLogger{
			Output:             os.Stdout,
			EnableRequestBody:  true,
			EnableResponseBody: true,
		}
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return es, nil
}

// Ping the Elasticsearch server.
func Ping(ctx context.Context, es *elasticsearch.Client) error {
	req := esapi.PingRequest{
		Pretty: true,
	}
	resp, err := req.Do(context.Background(), es)
	if err != nil {
		return fmt.Errorf("pinging: %w", err)
	}
	switch resp.StatusCode {
	default:
		return fmt.Errorf("checking state: %w [StatusCode=%d]", err, resp.StatusCode)
	case http.StatusOK:
		return nil // OK
	}
}
