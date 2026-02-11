package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	opsCount uint64
	uri      = flag.String("uri", "mongodb://localhost:27017", "MongoDB connection URI")
	workers  = flag.Int("workers", 75, "Number of concurrent workers")
	mode     = flag.String("mode", "sampleRate", "Mode: 'sampleRate' (Scatter-Gather) or 'bulk' (Targeted)")
	dbName   = flag.String("db", "load_test", "Database name")
)

func main() {
	flag.Parse()

	client, err := mongo.Connect(options.Client().ApplyURI(*uri))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.Background())

	fmt.Printf("Starting load [%s mode] with %d workers...\n", *mode, *workers)

	// Reporter: Prints ops/sec every second
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			ops := atomic.SwapUint64(&opsCount, 0)
			fmt.Printf("Current Throughput: %d ops/sec\n", ops)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runWorker(client, id)
		}(i)
	}
	wg.Wait()
}

func runWorker(client *mongo.Client, id int) {
	db := client.Database(*dbName)
	coll := db.Collection("load_test_coll")
	ctx := context.Background()

	// Update pipeline used in both modes
	pipeline := mongo.Pipeline{
		{{Key: "$set", Value: bson.M{
			"touchedBy":  id,
			"updatedAt":  "$$NOW",
			"visitCount": bson.M{"$add": bson.A{"$visitCount", 1}},
		}}},
	}

	for {
		if *mode == "sampleRate" {
			// Path 1: Scatter-Gather via $sampleRate Query Predicate
			res, err := coll.UpdateMany(ctx, bson.M{"$sampleRate": 0.01}, pipeline)
			if err == nil {
				atomic.AddUint64(&opsCount, uint64(res.ModifiedCount))
			}
		} else {
			// Path 2: Targeted Routing via $sample + BulkWrite
			// First: Fetch random IDs (uses aggregation engine)
			cursor, _ := coll.Aggregate(ctx, mongo.Pipeline{
				{{Key: "$sample", Value: bson.M{"size": 100}}},
				{{Key: "$project", Value: bson.M{"_id": 1}}},
			})

			var results []struct {
				ID interface{} `bson:"_id"`
			}
			_ = cursor.All(ctx, &results)

			if len(results) > 0 {
				models := make([]mongo.WriteModel, len(results))
				for i, doc := range results {
					models[i] = mongo.NewUpdateOneModel().
						SetFilter(bson.M{"_id": doc.ID}).
						SetUpdate(pipeline)
				}
				// Unordered BulkWrite allows the driver to hit all shards in parallel
				res, err := coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
				if err == nil {
					atomic.AddUint64(&opsCount, uint64(res.ModifiedCount))
				}
			}
		}
	}
}
