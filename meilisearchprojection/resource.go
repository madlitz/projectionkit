package meilisearchprojection

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/dogmatiq/projectionkit/resource"
	"github.com/meilisearch/meilisearch-go"
)

// ResourceRepository is an implementation of resource.Repository that stores
// resources versions in a Meilisearch database.
type ResourceRepository struct {
	db       meilisearch.ServiceManager
	key      string
	occTable string
}

type resourceDoc struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

var _ resource.Repository = (*ResourceRepository)(nil)

func hash(data ...string) string {
	hash := sha256.New()
	for _, d := range data {
		hash.Write([]byte(d))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// NewResourceRepository returns a new Meilisearch resource repository.
func NewResourceRepository(
	db meilisearch.ServiceManager,
	key, occTable string,
) *ResourceRepository {
	return &ResourceRepository{db, key, occTable}
}

// ResourceVersion returns the version of the resource r.
func (rr *ResourceRepository) ResourceVersion(ctx context.Context, r []byte) ([]byte, error) {

	occIndex := rr.db.Index(rr.occTable)
	resourceID := hash(rr.key, string(r))

	var doc resourceDoc
	err := occIndex.GetDocumentWithContext(ctx, resourceID, nil, &doc)
	if err != nil {
		if merr, ok := err.(*meilisearch.Error); ok {
			if merr.StatusCode == 404 {
				return nil, nil
			}
		}
		return nil, err
	}

	return []byte(doc.Version), nil

}

// StoreResourceVersion sets the version of the resource r to v without checking
// the current version.
func (rr *ResourceRepository) StoreResourceVersion(ctx context.Context, r, v []byte) error {

	occIndex := rr.db.Index(rr.occTable)
	resourceID := hash(rr.key, string(r))

	task, err := occIndex.AddDocumentsWithContext(ctx, []map[string]string{
		{
			"id":      resourceID,
			"version": string(v),
		},
	})
	if err != nil {
		return err
	}

	err = waitForTask(ctx, rr.db, task.TaskUID)
	if err != nil {
		return err
	}

	return nil
}

// UpdateResourceVersion updates the version of the resource r to n.
//
// If c is not the current version of r, it returns false and no update occurs.
func (rr *ResourceRepository) UpdateResourceVersion(
	ctx context.Context,
	r, c, n []byte,
) (ok bool, err error) {

	return rr.updateResourceVersion(ctx, r, c, n)

}

// UpdateResourceVersion updates the version of the resource r to n and performs
// a user-defined operation within the same transaction.
//
// If c is not the current version of r, it returns false and no update occurs.
func (rr *ResourceRepository) UpdateResourceVersionFn(
	ctx context.Context,
	r, c, n []byte,
	fn func(context.Context, meilisearch.IndexManager) (bool, error),
) (ok bool, err error) {

	ok, err = fn(ctx, rr.db.Index(rr.key))
	if err != nil {
		return false, err
	}

	if !ok {
		return false, nil
	}

	ok, err = rr.updateResourceVersion(ctx, r, c, n)
	if !ok || err != nil {
		return false, err
	}

	return true, nil

}

// UpdateResourceVersion updates the version of the resource r to n.
//
// If c is not the current version of r, it returns false and no update occurs.
func (rr *ResourceRepository) updateResourceVersion(ctx context.Context,
	r, c, n []byte,
) (ok bool, err error) {

	occIndex := rr.db.Index(rr.occTable)
	resourceID := hash(rr.key, string(r))

	var doc resourceDoc
	err = occIndex.GetDocumentWithContext(ctx, resourceID, nil, &doc)
	if err != nil {
		if merr, ok := err.(*meilisearch.Error); ok {
			if merr.StatusCode != 404 {
				return false, err
			}
		}
	}

	if len(c) == 0 {
		// If current version is not provided, check if the resource exists.
		if doc.ID == "" {
			// If resource does not exist, create it with the new version.
			task, err := occIndex.AddDocumentsWithContext(ctx, []map[string]string{
				{
					"id":      resourceID,
					"version": string(n),
				},
			})
			if err != nil {
				return false, err
			}

			err = waitForTask(ctx, rr.db, task.TaskUID)
			if err != nil {
				return false, err
			}

			return true, nil

		}

		return false, nil
	}

	// If the resource does exist, update it only if the current version matches.
	if doc.Version == string(c) {
		task, err := occIndex.UpdateDocumentsWithContext(ctx, []resourceDoc{
			{
				ID:      resourceID,
				Version: string(n),
			},
		})
		if err != nil {
			return false, err
		}

		err = waitForTask(ctx, rr.db, task.TaskUID)
		if err != nil {
			return false, err
		}

		return true, nil
	}

	return false, nil
}

// DeleteResource removes all information about the resource r.
func (rr *ResourceRepository) DeleteResource(ctx context.Context, r []byte) error {

	occIndex := rr.db.Index(rr.occTable)
	resourceID := hash(rr.key, string(r))

	task, err := occIndex.DeleteDocumentWithContext(ctx, resourceID)
	if err != nil {
		return err
	}

	err = waitForTask(ctx, rr.db, task.TaskUID)
	if err != nil {
		return err
	}

	return nil

}

func waitForTask(ctx context.Context, db meilisearch.ServiceManager, taskID int64) error {
	for {
		task, err := db.GetTaskWithContext(ctx, taskID)
		if err != nil {
			return err
		}
		if task.Status == "succeeded" {
			return nil
		}
		if task.Status == "failed" {
			return fmt.Errorf("task failed: %v", task.Error)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
