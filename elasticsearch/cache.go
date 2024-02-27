package elasticsearch

import (
	"sync"
)

// ContainerCache is a thread-safe cache for elasticsearch containers.
type ContainerCache struct {
	mu    sync.Mutex
	cache map[string]*Container
}

// NewContainerCache returns a new ContainerCache.
func NewContainerCache() *ContainerCache {
	return &ContainerCache{
		cache: make(map[string]*Container),
	}
}

// Close stops all containers in the cache.
func (p *ContainerCache) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, c := range p.cache {
		if err := c.Close(); err != nil {
			return err
		}
	}

	p.cache = make(map[string]*Container)

	return nil
}

// GetOrCreate starts a new container if none is running, otherwise returns
// the pooled container.
func (p *ContainerCache) GetOrCreate(id string, createFunc func() *Container) *Container {
	p.mu.Lock()
	defer p.mu.Unlock()

	if c, ok := p.cache[id]; ok {
		return c
	}

	c := createFunc()
	p.cache[id] = c

	return c
}
