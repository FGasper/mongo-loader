package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func runPruneData(ctx context.Context, cmd any, fraction float64) (retErr error) {
	if fraction < 0 || fraction > 1 {
		return fmt.Errorf("fraction must be between 0 and 1, got %.4f", fraction)
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()

	database := client.Database("test")

	// Get all collections
	collections, err := database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("list collections: %w", err)
	}

	if len(collections) == 0 {
		slog.Info("No collections found")
		return nil
	}

	setupLogging()
	slog.Info("Starting prune-data", "fraction", fraction, "collections", len(collections))

	totalDeleted := int64(0)

	for _, collName := range collections {
		coll := database.Collection(collName)

		// Get count
		count, err := coll.EstimatedDocumentCount(ctx)
		if err != nil {
			return fmt.Errorf("count %s: %w", collName, err)
		}

		if count == 0 {
			slog.Info("Collection is empty, skipping", "collection", collName)
			continue
		}

		deleteCount := int64(float64(count) * fraction)
		if deleteCount == 0 {
			slog.Debug("No documents to delete", "collection", collName, "deleteCount", deleteCount)
			continue
		}

		slog.Info("Pruning collection", "collection", collName, "total", count, "toDelete", deleteCount)

		// Fetch all _ids in batches
		var ids []interface{}
		opts := options.Find().SetBatchSize(10000)
		cursor, err := coll.Find(ctx, bson.D{}, opts)
		if err != nil {
			return fmt.Errorf("find %s: %w", collName, err)
		}
		defer cursor.Close(ctx)

		type idDoc struct {
			ID interface{} `bson:"_id"`
		}

		for cursor.Next(ctx) {
			var doc idDoc
			if err := cursor.Decode(&doc); err != nil {
				return fmt.Errorf("decode %s: %w", collName, err)
			}
			ids = append(ids, doc.ID)
		}

		if err := cursor.Err(); err != nil {
			return fmt.Errorf("cursor error %s: %w", collName, err)
		}

		// Randomly shuffle
		rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })

		// Delete selected documents
		idsToDelete := ids[:deleteCount]
		result, err := coll.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": idsToDelete}})
		if err != nil {
			return fmt.Errorf("delete %s: %w", collName, err)
		}

		totalDeleted += result.DeletedCount
		slog.Info("Deleted documents", "collection", collName, "deleted", result.DeletedCount)
	}

	slog.Info("Prune complete", "totalDeleted", totalDeleted)
	return nil
}
