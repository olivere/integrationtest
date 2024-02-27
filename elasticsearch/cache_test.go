package elasticsearch_test

import (
	"context"
	"testing"
	"time"

	"github.com/olivere/integrationtest/elasticsearch"
)

func TestContainerCache_Start(t *testing.T) {
	cache := elasticsearch.NewContainerCache()
	defer cache.Close()

	// Start container
	now := time.Now()
	c1 := cache.GetOrCreate("one", func() *elasticsearch.Container {
		return elasticsearch.Start(t, elasticsearch.WithTimeout(10*time.Second))
	})
	startupC1 := time.Since(now)
	defer c1.Close()

	// Start container
	now = time.Now()
	c2 := cache.GetOrCreate("one", func() *elasticsearch.Container {
		return elasticsearch.Start(t, elasticsearch.WithTimeout(10*time.Second))
	})
	startupC2 := time.Since(now)
	defer c2.Close()

	if c1 != c2 {
		t.Fatalf("expected same container, got different")
	}

	t.Logf("startupC1 = %v, startupC2 = %v", startupC1, startupC2)

	if startupC2 > startupC1 {
		t.Fatalf("expected container to be reused, got new container")
	}

	// Ping database on c1
	{
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := elasticsearch.Ping(ctx, c1.Client()); err != nil {
			t.Fatalf("could not ping database: %v", err)
		}
	}

	// Ping database on c2
	{
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := elasticsearch.Ping(ctx, c2.Client()); err != nil {
			t.Fatalf("could not ping database: %v", err)
		}
	}

	// Stop container
	if err := cache.Close(); err != nil {
		t.Fatalf("could not stop container: %v", err)
	}

	// Ping database on c1
	{
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := elasticsearch.Ping(ctx, c1.Client()); err == nil {
			t.Fatalf("expected error, got nil")
		}
	}

	// Ping database on c2
	{
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := elasticsearch.Ping(ctx, c2.Client()); err == nil {
			t.Fatalf("expected error, got nil")
		}
	}
}
