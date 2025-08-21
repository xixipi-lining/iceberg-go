package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go/catalog"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/cenkalti/backoff/v4"
)

func (c *Catalog) SetKVSidecar(ctx context.Context, key, value string) error {
	item, err := (&dynamodbKVSidecarItem{
		Name:  key,
		Value: value,
	}).MarshalMap()
	if err != nil {
		return fmt.Errorf("failed to marshal item: %w", err)
	}

	_, err = c.dynamodb.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(c.tableName),
	})
	if err != nil {
		return fmt.Errorf("failed to put item: %w", err)
	}

	return nil
}

func (c *Catalog) GetKVSidecar(ctx context.Context, key string) (string, error) {
	res, err := c.dynamodb.GetItem(ctx, &dynamodb.GetItemInput{
		Key: (&dynamodbKVSidecarItem{
			Name: key,
		}).Key(),
		TableName: aws.String(c.tableName),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get item: %w", err)
	}

	item := &dynamodbKVSidecarItem{}
	if err := item.UnmarshalMap(res.Item); err != nil {
		return "", fmt.Errorf("failed to unmarshal item: %w", err)
	}

	return item.Value, nil
}


func (c *Catalog) Transaction(ctx context.Context, reqs []catalog.TransactionRequest, followers ...catalog.FollowerCatalog) error {
	transactItems := make([]types.TransactWriteItem, len(reqs))

	fins := make([]func() error, 0, len(reqs))

	for _, req := range reqs {
		switch req := req.(type) {
		case *catalog.CreateTableRequest:
			ns := strings.Join(catalog.NamespaceFromIdent(req.Identifier), ".")
			exists, err := c.namespaceExists(ctx, ns)
			if err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, ns)
			}

			staged, err := c.stageCreateTable(ctx, req.Identifier, req.Schema, req.Opts...)
			if err != nil {
				return err
			}

			item, err := c.getCreateTablePutItem(staged)
			if err != nil {
				return err
			}

			for _, f := range followers {
				fins = append(fins, func() error {
					return f.CreateTableByTable(ctx, staged)
				})
			}

			transactItems = append(transactItems, types.TransactWriteItem{
				Put: &types.Put{
					TableName:                 item.TableName,
					Item:                      item.Item,
					ConditionExpression:       item.ConditionExpression,
					ExpressionAttributeNames:  item.ExpressionAttributeNames,
					ExpressionAttributeValues: item.ExpressionAttributeValues,
				},
			})
		case *catalog.CommitTableRequest:
			current, staged, err := c.stageCommitTable(ctx, req.Table, req.Requirements, req.Updates)
			if err != nil {
				if errors.Is(err, ErrNoChanges) {
					continue
				}
				return err
			}

			updateItem, err := c.getCommitTableUpdateItem(current, staged)
			if err != nil {
				return err
			}

			for _, f := range followers {
				fins = append(fins, func() error {
					return f.CommitTableByTable(ctx, current, staged)
				})
			}

			transactItems = append(transactItems, types.TransactWriteItem{
				Update: &types.Update{
					TableName:                 updateItem.TableName,
					Key:                       updateItem.Key,
					ConditionExpression:       updateItem.ConditionExpression,
					UpdateExpression:          updateItem.UpdateExpression,
					ExpressionAttributeNames:  updateItem.ExpressionAttributeNames,
					ExpressionAttributeValues: updateItem.ExpressionAttributeValues,
				},
			})
		case *catalog.SetKVSidecarRequest:
			item, err := (&dynamodbKVSidecarItem{
				Name:  req.Key,
				Value: req.Value,
			}).MarshalMap()
			if err != nil {
				return fmt.Errorf("failed to marshal item: %w", err)
			}
			transactItems = append(transactItems, types.TransactWriteItem{
				Put: &types.Put{
					TableName: aws.String(c.tableName),
					Item:      item,
				},
			})
		}
	}

	_, err := c.dynamodb.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		return fmt.Errorf("failed to transact: %w", err)
	}

	go executeFollowerInstructions(fins)

	return nil
}

// executeFollowerTransactionsConcurrently executes all follower transaction functions concurrently
// with exponential backoff retry and a maximum execution time of 1 minute.
func executeFollowerInstructions(fins []func() error) {
	if len(fins) == 0 {
		return
	}

	// Create a context with 1-minute timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var wg sync.WaitGroup
	
	for _, fin := range fins {
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
		}(fin)
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

var _ catalog.TransactionCatalog = (*Catalog)(nil)