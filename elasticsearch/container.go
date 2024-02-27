package elasticsearch

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

type Container struct {
	c        *elasticsearch.Client
	timeout  time.Duration
	hostPort string
	pool     *dockertest.Pool
	resource *dockertest.Resource

	mu     sync.Mutex
	closed bool
}

type startConfig struct {
	timeout   time.Duration
	postStart []postStartFunc
}

type startConfigFunc func(*startConfig)

type postStartFunc func(*Container) error

func WithTimeout(timeout time.Duration) startConfigFunc {
	return func(cfg *startConfig) {
		cfg.timeout = timeout
	}
}

// WithPostStart adds a post-startup operation to the container.
// This can be used to install extensions, create tables, seed data etc.
func WithPostStart(funcs ...postStartFunc) startConfigFunc {
	return func(cfg *startConfig) {
		cfg.postStart = funcs
	}
}

// Start an Elasticsearch cluster/node.
func Start(tb testing.TB, options ...startConfigFunc) *Container {
	tb.Helper()

	startCfg := startConfig{}
	for _, o := range options {
		o(&startCfg)
	}

	timeout := startCfg.timeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}

	c := &Container{
		timeout: timeout,
	}

	var err error
	c.pool, err = dockertest.NewPool("")
	if err != nil {
		tb.Fatalf("unable to connect to Docker: %v", err)
	}
	if err = c.pool.Client.Ping(); err != nil {
		tb.Fatalf(`could not connect to docker: %v`, err)
	}

	env := []string{
		"node.name=elasticsearch-test",
		"cluster.name=elasticsearch-test",
		"discovery.type=single-node",
		"logger.org.elasticsearch=warn",
		"bootstrap.memory_lock=true",
		"xpack.security.enabled=false",
		"xpack.license.self_generated.type=basic",
		"ingest.geoip.downloader.enabled=false",
	}

	c.resource, err = c.pool.RunWithOptions(&dockertest.RunOptions{
		Name:       fmt.Sprintf("elasticsearch_%09d", time.Now().UnixNano()),
		Repository: "docker.elastic.co/elasticsearch/elasticsearch",
		Tag:        "8.12.2",
		Env:        env,
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.NeverRestart()
		config.Memory = 1 * 1024 * 1024 * 1024 // 1GB
		config.Ulimits = []docker.ULimit{
			{
				Name: "memlock",
				Soft: -1,
				Hard: -1,
			},
		}
	})
	if err != nil {
		tb.Fatalf("unable to start Elasticsearch container: %v", err)
	}
	tb.Cleanup(func() {
		c.Close()
	})

	// Tell docker to hard kill the container in "timeout" seconds
	if err := c.resource.Expire(uint(timeout.Seconds())); err != nil {
		tb.Fatal(err)
	}
	c.pool.MaxWait = timeout

	c.hostPort = c.resource.GetHostPort("9200/tcp")

	c.c, err = Connect(context.Background(), fmt.Sprintf("http://%s", c.hostPort))
	if err != nil {
		tb.Fatalf("could not connect to Elasticsearch container: %v", err)
	}
	err = c.pool.Retry(func() (err error) {
		req := esapi.PingRequest{
			Pretty: true,
		}
		resp, err := req.Do(context.Background(), c.c)
		if err != nil {
			return fmt.Errorf("pinging: %w", err)
		}
		switch resp.StatusCode {
		default:
			return fmt.Errorf("checking state: %w [StatusCode=%d]", err, resp.StatusCode)
		case http.StatusOK:
			return nil // OK
		}
	})
	if err != nil {
		tb.Fatalf("could not ping Elasticsearch container: %v", err)
	}

	// Run all post-startup operations
	for _, f := range startCfg.postStart {
		err = f(c)
		if err != nil {
			tb.Fatalf("could not run post-startup operation: %v", err)
		}
	}

	return c
}

func (c *Container) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	err := c.pool.Purge(c.resource)
	if err != nil {
		return fmt.Errorf("could not purge containers: %w", err)
	}

	c.closed = true

	return nil
}

func (c *Container) Client() *elasticsearch.Client {
	return c.c
}
