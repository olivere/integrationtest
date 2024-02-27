package elasticsearch_test

import (
	"context"
	"testing"
	"time"

	"github.com/olivere/integrationtest/elasticsearch"
)

func TestContainer_Start(t *testing.T) {
	c := elasticsearch.Start(t, elasticsearch.WithTimeout(10*time.Second))
	defer c.Close()

	es := c.Client()

	// Ping database
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := elasticsearch.Ping(ctx, es); err != nil {
		t.Fatalf("could not ping database: %v", err)
	}

	// Stop container
	if err := c.Close(); err != nil {
		t.Fatalf("could not stop container: %v", err)
	}

	// Ping database
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := elasticsearch.Ping(ctx, es); err == nil {
		t.Fatalf("expected error, got nil")
	}
}
