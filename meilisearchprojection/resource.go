package meilisearchprojection

import (
	"context"
	"fmt"

	"github.com/dogmatiq/projectionkit/resource"
	"github.com/meilisearch/meilisearch-go"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// ResourceRepository is an implementation of resource.Repository that stores
// resources versions in a Meilisearch database.
type ResourceRepository struct {
	db       meilisearch.ServiceManager
	key      string
	occTable string
}

var _ resource.Repository = (*ResourceRepository)(nil)

// NewResourceRepository returns a new Meilisearch resource repository.
func NewResourceRepository(
	db meilisearch.ServiceManager,
	key, occTable string,
) *ResourceRepository {
	return &ResourceRepository{db, key, occTable}
}

// ResourceVersion returns the version of the resource r.
func (rr *ResourceRepository) ResourceVersion(ctx context.Context, r []byte) ([]byte, error) {

	index := meilisearch.

	index

		fmt.Sprintf(`MATCH (p:%s{handler: $handler, resource: $resource} )
		RETURN p.version`, rr.occTable),
		map[string]any{
			"handler":  rr.key,
			"resource": string(r),
		},
	)

	if err != nil {
		return nil, err
	}

	if result.Next(ctx) {
		return []byte(result.Record().Values[0].(string)), nil
	}

	return nil, nil

}

// StoreResourceVersion sets the version of the resource r to v without checking
// the current version.
func (rr *ResourceRepository) StoreResourceVersion(ctx context.Context, r, v []byte) error {

	session := rr.db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	_, err := session.Run(ctx,
		fmt.Sprintf(`MERGE (p:%s{handler: $handler, resource: $resource} )
			SET p.version = $version
			RETURN p.version`, rr.occTable,
		),
		map[string]any{
			"version":  string(v),
			"handler":  rr.key,
			"resource": string(r),
		})
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

	return rr.withTx(ctx, func(tx neo4j.ExplicitTransaction) (bool, error) {
		return rr.updateResourceVersion(ctx, tx, r, c, n)
	})

}

// UpdateResourceVersion updates the version of the resource r to n and performs
// a user-defined operation within the same transaction.
//
// If c is not the current version of r, it returns false and no update occurs.
func (rr *ResourceRepository) UpdateResourceVersionFn(
	ctx context.Context,
	r, c, n []byte,
	fn func(context.Context, meilisearch.ServiceManager) (bool, error),
) (ok bool, err error) {
	return rr.withTx(ctx, func(db meilisearch.ServiceManager) (bool, error) {
		ok, err = rr.updateResourceVersion(ctx, db, r, c, n)
		if !ok || err != nil {
			return false, err
		}

		return fn(ctx, db)
	})
}

// UpdateResourceVersion updates the version of the resource r to n.
//
// If c is not the current version of r, it returns false and no update occurs.
func (rr *ResourceRepository) updateResourceVersion(ctx context.Context,
	db meilisearch.ServiceManager,
	r, c, n []byte,
) (ok bool, err error) {

	var result neo4j.ResultWithContext

	// If the new version is empty, delete the resource only if the current version matches.
	if len(n) == 0 {
		// First see if the resource exists with the current version.
		result, err = tx.Run(ctx,
			fmt.Sprintf(`MATCH (p:%s{handler: $handler, resource: $resource, version: $current_version} )
			RETURN p`, rr.occTable),
			map[string]any{
				"handler":         rr.key,
				"resource":        string(r),
				"current_version": string(c),
			},
		)
		if err != nil {
			return false, err
		}
		exists := result.Next(ctx)

		// Now delete the resource if it exists.
		_, err = tx.Run(ctx,
			fmt.Sprintf(`MATCH (p:%s{handler: $handler, resource: $resource, version: $current_version} )
			DETACH DELETE p`, rr.occTable),
			map[string]any{
				"handler":         rr.key,
				"resource":        string(r),
				"current_version": string(c),
			},
		)
		if err != nil {
			return false, err
		}

		fmt.Printf("Deleting resource %v, version %v: %v \n", string(r), string(c), ok)
		return exists, nil
	}

	// If resource does not exist, create it with the new version.
	result, err = tx.Run(ctx,
		fmt.Sprintf(`MATCH (p:%s{handler: $handler, resource: $resource} )
		RETURN p`, rr.occTable),
		map[string]any{
			"handler":  rr.key,
			"resource": string(r),
		},
	)
	if err != nil {
		return false, err
	}

	// If resource does not exist, create it unconditionally
	exists := result.Next(ctx)
	if !exists && len(c) == 0 {
		_, err = tx.Run(ctx,
			fmt.Sprintf(`CREATE (p:%s{handler: $handler, resource: $resource, version: $version})`, rr.occTable),
			map[string]any{
				"handler":  rr.key,
				"resource": string(r),
				"version":  string(n),
			},
		)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	// If the resource does exist, update it only if the current version matches.
	result, err = tx.Run(ctx,
		fmt.Sprintf(`MATCH (p:%s{handler: $handler, resource: $resource, version: $current_version})
			SET p.version = $new_version
			RETURN p`, rr.occTable,
		),
		map[string]any{
			"current_version": string(c),
			"new_version":     string(n),
			"handler":         rr.key,
			"resource":        string(r),
		},
	)
	if err != nil {
		return false, err
	}

	return result.Next(ctx), nil
}

// DeleteResource removes all information about the resource r.
func (rr *ResourceRepository) DeleteResource(ctx context.Context, r []byte) error {

	session := rr.db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	_, err := session.Run(ctx,
		fmt.Sprintf(`MATCH (p:%s{handler: $handler, resource: $resource})
		DELETE p`, rr.occTable),
		map[string]interface{}{
			"handler":  rr.key,
			"resource": string(r),
		})
	if err != nil {
		return err
	}

	return nil

}

// withTx calls fn on rr.db.
//
// fn is called within a transaction. The transaction is committed if fn returns
// ok; otherwise, it is rolled back.
func (rr *ResourceRepository) withTx(
	ctx context.Context,
	fn func(meilisearch.ServiceManager) (bool, error),
) (bool, error) {
	var ok bool

	txID := uuid.New().String()

	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) // nolint:errcheck

	ok, err = fn(index, txID)
	if err != nil {
		return false, err
	}

	if ok {
		return true, tx.Commit(ctx)
	}

	return false, tx.Rollback(ctx)
}
