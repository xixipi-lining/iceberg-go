package catalog

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/cenkalti/backoff/v4"
)

type OperationCreateTable struct {
	Identifier table.Identifier
	Schema     *iceberg.Schema
	Opts       []CreateTableOpt
}

type OperationCommitTable struct {
	Identifier   table.Identifier
	Requirements []table.Requirement
	Updates      []table.Update
}

type OperationSetKVSidecar struct {
	Key   string
	Value string
}

type Operation interface {
	transactionOperation()
}

func (c *OperationCreateTable) transactionOperation()  {}
func (c *OperationCommitTable) transactionOperation()  {}
func (c *OperationSetKVSidecar) transactionOperation() {}

type FollowerCatalog interface {
	CreateTableByTable(ctx context.Context, staged *table.StagedTable) error
	CommitTableByTable(ctx context.Context, current *table.Table, staged *table.StagedTable) error
}

type TransactionCatalog interface {
	Catalog
	SetKVSidecar(ctx context.Context, key, value string) error
	GetKVSidecar(ctx context.Context, key string) (string, error)
	Transaction(ctx context.Context, operations []Operation) error
}

// executeFollowerTransactionsConcurrently executes all follower transaction functions concurrently
// with exponential backoff retry and a maximum execution time of 1 minute.
func ExecuteFollowerInstructions(instructions []func() error) {
	if len(instructions) == 0 {
		return
	}

	// Create a context with 1-minute timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var wg sync.WaitGroup

	for _, ins := range instructions {
		wg.Add(1)
		go func(f func() error) {
			defer wg.Done()

			// Configure exponential backoff
			exponentialBackOff := backoff.NewExponentialBackOff()
			exponentialBackOff.InitialInterval = 100 * time.Millisecond
			exponentialBackOff.RandomizationFactor = 0.5
			exponentialBackOff.Multiplier = 2.0
			exponentialBackOff.MaxInterval = 10 * time.Second
			exponentialBackOff.MaxElapsedTime = time.Minute

			// Create backoff with context to respect the timeout
			backoffWithContext := backoff.WithContext(exponentialBackOff, ctx)

			// Execute the function with exponential backoff
			err := backoff.Retry(f, backoffWithContext)
			if err != nil {
				log.Printf("failed to execute follower transaction after retries: %v", err)
			}
		}(ins)
	}

	// Wait for all goroutines to complete or timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All functions completed
	case <-ctx.Done():
		log.Printf("follower transactions execution timed out after 1 minute")
	}
}
