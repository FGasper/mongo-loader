package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	// 1 TiB
	oneTiB = 1 << 40

	// Name of the DB to use (equivalent to "db" in the shell)
	dbName = "test"

	// Same as JS: 0.005
	fraction = 0.005
)

var stopRequested int32

type writesSummary struct {
	PlainInserts int64 `json:"plainInserts,omitempty"`
	PlainDeletes int64 `json:"plainDeletes,omitempty"`
}

func main() {
	ctx := context.Background()

	// Seed global RNG similar to Math.random()
	rand.Seed(time.Now().UnixNano())

	// Handle SIGUSR2 to stop writes cleanly
	setupSignalHandler()

	// Connect to MongoDB
	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("failed to connect to MongoDB: %v", err)
	}
	defer func() {
		_ = client.Disconnect(context.Background())
	}()

	db := client.Database(dbName)

	// Equivalent of getShardNames().length
	shardNames, err := getShardNames(ctx, client)
	if err != nil {
		log.Fatalf("failed to list shards: %v", err)
	}

	totalDataSize := int64(oneTiB) * int64(len(shardNames))
	if totalDataSize == 0 {
		log.Fatalf("Huh?? 0 shards??")
	}

	docSizes := []int{500, 1000, 2000}
	customIDModes := []bool{true, false}
	_ = totalDataSize / int64(len(docSizes)) / int64(len(customIDModes)) // collectionSize; not used in original script either

	// Equivalent of:
	// const versionArray = db.version().split('.').map(s => parseInt(s, 10));
	major, minor, err := getServerVersion(ctx, db)
	if err != nil {
		log.Fatalf("failed to get server version: %v", err)
	}

	canTimeseries := major >= 5
	tsCanDeleteWithoutMeta := major >= 7
	_ = canTimeseries
	_ = tsCanDeleteWithoutMeta

	canUpdateWithPipeline := major >= 5 || (major == 4 && minor == 4)

	// Build the update pipeline equivalent to JS `updatePipeline`
	updatePipeline := buildUpdatePipeline()

	// Map of collection name -> old document count
	allOldDocsCounts := make(map[string]int64)

	// Infinite loop like `while (true) { ... }`
	for atomic.LoadInt32(&stopRequested) == 0 {
		for _, docSize := range docSizes {
			for _, useCustomID := range customIDModes {
				if atomic.LoadInt32(&stopRequested) != 0 {
					break
				}

				collName := fmt.Sprintf("%s_%d", func() string {
					if useCustomID {
						return "customID"
					}
					return "sequentialID"
				}(), docSize)

				coll := db.Collection(collName)

				startTime := time.Now()

				writes := writesSummary{}
				newDocsCount := int64(50_000)

				// Initialize old document count once per collection
				oldCount, ok := allOldDocsCounts[collName]
				if !ok {
					oldCount, err = coll.EstimatedDocumentCount(ctx)
					if err != nil {
						log.Printf("%s: failed to estimate document count: %v", collName, err)
						oldCount = 0
					}
					allOldDocsCounts[collName] = oldCount
				}
				oldDocsCount := allOldDocsCounts[collName]

				// Build new docs
				newDocs := make([]interface{}, 0, newDocsCount)
				for i := int64(0); i < newDocsCount; i++ {
					doc := bson.M{
						"_id":         nil, // default; overwritten if useCustomID
						"rand":        rand.Float64(),
						"str":         repeatString("y", docSize),
						"fromUpdates": true,
					}
					if useCustomID {
						doc["_id"] = rand.Float64() // Math.random()
					} else {
						delete(doc, "_id")
					}
					newDocs = append(newDocs, doc)
				}

				log.Printf("%s: Inserting %d documents …", collName, newDocsCount)

				// Insert many (ordered: false)

				opts := options.InsertMany().SetOrdered(false)
				res, err := coll.InsertMany(ctx, newDocs, opts)
				if err != nil {
					log.Printf("%s: Failed to insert: %v", collName, err)
					time.Sleep(3 * time.Second)
					return
				}
				writes.PlainInserts = int64(len(res.InsertedIDs))

				// Updates
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("%s: panic during updates: %v", collName, r)
						}
					}()

					if canUpdateWithPipeline {
						log.Printf("%s: Updating random documents via pipeline …", collName)

						cmd := bson.D{
							{"update", collName},
							{"updates", bson.A{
								bson.D{
									{"q", bson.D{{"$sampleRate", 0.001}}},
									{"u", updatePipeline}, // mongo.Pipeline → encodes as array, same as your JS updatePipeline
									{"multi", true},
								},
							}},
							{"ordered", false},
						}

						if err := db.RunCommand(ctx, cmd).Err(); err != nil {
							log.Printf("%s: Failed to run pipeline update: %v", collName, err)
							return
						}
					} else {
						// keep your existing non-pipeline branch:
						log.Printf("%s: Fetching %d random document IDs …", collName, newDocsCount)

						pipeline := mongo.Pipeline{
							{{"$sample", bson.D{{"size", newDocsCount}}}},
							{{"$project", bson.D{{"_id", 1}}}},
						}

						cur, err := coll.Aggregate(ctx, pipeline)
						if err != nil {
							log.Printf("%s: Failed to fetch IDs for update: %v", collName, err)
							time.Sleep(3 * time.Second)
							return
						}
						var docs []struct {
							ID interface{} `bson:"_id"`
						}
						if err := cur.All(ctx, &docs); err != nil {
							log.Printf("%s: Failed to decode IDs for update: %v", collName, err)
							time.Sleep(3 * time.Second)
							return
						}

						ids := make([]interface{}, 0, len(docs))
						for _, d := range docs {
							ids = append(ids, d.ID)
						}

						log.Printf("%s: Updating those randomly …", collName)

						updates := bson.A{}
						for _, id := range ids {
							updates = append(updates, bson.D{
								{"q", bson.D{{"_id", id}}},
								{"u", createRandomUpdate()},
							})
						}

						cmd := bson.D{
							{"update", collName},
							{"updates", updates},
							{"writeConcern", bson.D{
								{"w", "majority"},
								{"j", true},
							}},
						}

						if err := db.RunCommand(ctx, cmd).Err(); err != nil {
							log.Printf("%s: Failed to run update command: %v", collName, err)
							time.Sleep(3 * time.Second)
							return
						}
					}
				}()

				// Deletes
				writes.PlainDeletes = 0

				for {
					if atomic.LoadInt32(&stopRequested) != 0 {
						break
					}

					currCount, err := coll.EstimatedDocumentCount(ctx)
					if err != nil {
						log.Printf("%s: Failed to estimate count before delete: %v", collName, err)
						break
					}
					excess := currCount - oldDocsCount
					if excess < 1 {
						break
					}

					if canUpdateWithPipeline {
						log.Printf("%s: Deleting about %d random documents …", collName, newDocsCount)

						res, err := coll.DeleteMany(ctx, bson.D{
							{Key: "$sampleRate", Value: 0.0001},
						})
						if err != nil {
							log.Printf("%s: Failed to delete: %v", collName, err)
							break
						}
						writes.PlainDeletes += res.DeletedCount
					} else {
						log.Printf("%s: Fetching %d random document IDs …", collName, newDocsCount)

						pipeline := mongo.Pipeline{
							{{Key: "$sample", Value: bson.D{{Key: "size", Value: newDocsCount}}}},
							{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
						}
						cur, err := coll.Aggregate(ctx, pipeline)
						if err != nil {
							log.Printf("%s: Failed to fetch IDs for delete: %v", collName, err)
							break
						}
						var docs []struct {
							ID interface{} `bson:"_id"`
						}
						if err := cur.All(ctx, &docs); err != nil {
							log.Printf("%s: Failed to decode IDs for delete: %v", collName, err)
							break
						}

						ids := make([]interface{}, 0, len(docs))
						for _, d := range docs {
							ids = append(ids, d.ID)
						}

						log.Printf("%s: Deleting those %d documents …", collName, len(ids))

						res, err := coll.DeleteMany(ctx, bson.D{
							{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}},
						})
						if err != nil {
							log.Printf("%s: Failed to delete: %v", collName, err)
							break
						}
						writes.PlainDeletes += res.DeletedCount
					}
				}

				elapsedSecs := time.Since(startTime).Seconds()
				roundedSecs := math.Round(elapsedSecs*100) / 100

				writesJSON, _ := json.Marshal(writes)
				log.Printf("%s: Writes sent over %.2f secs: %s", collName, roundedSecs, string(writesJSON))
			}
		}
	}

	log.Println("Stop requested, exiting main loop.")
}

// ----------------------------------------------------------------------
// Helper functions – equivalents to various JS pieces
// ----------------------------------------------------------------------

// setupSignalHandler installs a handler for SIGUSR2 that sets stopRequested.
func setupSignalHandler() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR2)
	go func() {
		sig := <-ch
		log.Printf("Received signal %v, will stop after current iteration…", sig)
		atomic.StoreInt32(&stopRequested, 1)
	}()
}

// getShardNames is the Go equivalent of `getShardNames()` from the shell.
//
// It runs { listShards: 1 } against the admin DB and returns all shard _ids.
func getShardNames(ctx context.Context, client *mongo.Client) ([]string, error) {
	adminDB := client.Database("admin")
	var res struct {
		Shards []struct {
			ID string `bson:"_id"`
		} `bson:"shards"`
	}
	if err := adminDB.RunCommand(ctx, bson.D{{Key: "listShards", Value: 1}}).Decode(&res); err != nil {
		return nil, err
	}

	names := make([]string, 0, len(res.Shards))
	for _, s := range res.Shards {
		names = append(names, s.ID)
	}
	return names, nil
}

// getServerVersion is equivalent to parsing `db.version()` in the shell.
// It returns major, minor from the "version" field of { buildInfo: 1 }.
func getServerVersion(ctx context.Context, db *mongo.Database) (int, int, error) {
	var res struct {
		Version string `bson:"version"`
	}
	if err := db.RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&res); err != nil {
		return 0, 0, err
	}

	var major, minor int
	_, err := fmt.Sscanf(res.Version, "%d.%d", &major, &minor)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse version %q: %w", res.Version, err)
	}
	return major, minor, nil
}

// repeatString is equivalent to `"y".repeat(docSize)` in JS.
func repeatString(s string, n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, len(s)*n)
	bl := copy(b, []byte(s))
	for bl < len(b) {
		copy(b[bl:], b[:bl])
		bl *= 2
	}
	return string(b)
}

// createRandomUpdate is the Go equivalent of your JS createRandomUpdate().
func createRandomUpdate() bson.D {
	r := rand.Float64()

	if r < 0.2 {
		return bson.D{
			{Key: "$set", Value: bson.D{
				{Key: "touchedByProcess", Value: os.Getpid()},
				{Key: "updatedAt", Value: time.Now()},
			}},
		}
	}

	if r <= 0.4 {
		return bson.D{
			{Key: "$set", Value: bson.D{
				{Key: "flag", Value: r < 0.5},
			}},
		}
	}

	if r <= 0.6 {
		return bson.D{
			{Key: "$set", Value: bson.D{
				{Key: "score", Value: 1000 * rand.Float64()},
			}},
		}
	}

	if r <= 0.8 {
		return bson.D{
			{Key: "$inc", Value: bson.D{
				{Key: "visitCount", Value: 1},
			}},
		}
	}

	return bson.D{
		{Key: "$currentDate", Value: bson.D{
			{Key: "now", Value: true},
		}},
	}
}

// buildUpdatePipeline builds the Go version of the JS `updatePipeline` array.
func buildUpdatePipeline() mongo.Pipeline {
	pid := os.Getpid()

	return mongo.Pipeline{
		// {
		//   $addFields: { randVal: { $rand: {} } }
		// }
		{
			{Key: "$addFields", Value: bson.D{
				{Key: "randVal", Value: bson.D{{Key: "$rand", Value: bson.D{}}}},
			}},
		},
		// second $addFields with many $cond expressions
		{
			{Key: "$addFields", Value: bson.D{
				{
					Key: "touchedByProcess",
					Value: bson.D{
						{Key: "$cond", Value: bson.A{
							bson.D{{Key: "$lt", Value: bson.A{"$randVal", 0.2}}},
							pid,
							"$touchedByProcess",
						}},
					},
				},
				{
					Key: "updatedAt",
					Value: bson.D{
						{Key: "$cond", Value: bson.A{
							bson.D{{Key: "$lt", Value: bson.A{"$randVal", 0.2}}},
							"$$NOW",
							"$updatedAt",
						}},
					},
				},
				{
					Key: "flag",
					Value: bson.D{
						{Key: "$cond", Value: bson.A{
							bson.D{{Key: "$and", Value: bson.A{
								bson.D{{Key: "$gte", Value: bson.A{"$randVal", 0.2}}},
								bson.D{{Key: "$lt", Value: bson.A{"$randVal", 0.4}}},
							}}},
							bson.D{{Key: "$lt", Value: bson.A{bson.D{{Key: "$rand", Value: bson.D{}}}, 0.5}}},
							"$flag",
						}},
					},
				},
				{
					Key: "score",
					Value: bson.D{
						{Key: "$cond", Value: bson.A{
							bson.D{{Key: "$and", Value: bson.A{
								bson.D{{Key: "$gte", Value: bson.A{"$randVal", 0.4}}},
								bson.D{{Key: "$lt", Value: bson.A{"$randVal", 0.6}}},
							}}},
							bson.D{
								{Key: "$floor", Value: bson.D{
									{Key: "$multiply", Value: bson.A{
										bson.D{{Key: "$rand", Value: bson.D{}}},
										1000,
									}},
								}},
							},
							"$score",
						}},
					},
				},
				{
					Key: "visitCount",
					Value: bson.D{
						{Key: "$cond", Value: bson.A{
							bson.D{{Key: "$and", Value: bson.A{
								bson.D{{Key: "$gte", Value: bson.A{"$randVal", 0.6}}},
								bson.D{{Key: "$lt", Value: bson.A{"$randVal", 0.8}}},
							}}},
							bson.D{
								{Key: "$cond", Value: bson.A{
									bson.D{{Key: "$eq", Value: bson.A{
										bson.D{{Key: "$type", Value: "$visitCount"}},
										"missing",
									}}},
									1,
									bson.D{{Key: "$add", Value: bson.A{"$visitCount", 1}}},
								}},
							},
							"$visitCount",
						}},
					},
				},
				{
					Key: "archivedField",
					Value: bson.D{
						{Key: "$cond", Value: bson.A{
							bson.D{{Key: "$gte", Value: bson.A{"$randVal", 0.8}}},
							"$oldField",
							"$archivedField",
						}},
					},
				},
				{
					Key: "oldField",
					Value: bson.D{
						{Key: "$cond", Value: bson.A{
							bson.D{{Key: "$gte", Value: bson.A{"$randVal", 0.8}}},
							"$$REMOVE",
							"$oldField",
						}},
					},
				},
				{
					Key:   "randVal",
					Value: "$$REMOVE",
				},
			}},
		},
	}
}
