function clusterIsSharded() {
    try {
        sh.status();
        print("Cluster is sharded.");

        return true;
    } catch(e) {
        console.log(`Sharding commands failed … I guess this isn’t a sharded cluster? (${e})`)
    }

    return false;
}

function getShardNames() {
    return JSON.parse(JSON.stringify(sh.status())).shards.map(s => s._id);
}

function splitCollection(ns, shardKeyField, min, max) {
    const shardNames = getShardNames();

    // ---------------------------------------------------------------------------
    // Parse db and collection from ns
    const [dbName, collName] = ns.split(".");
    if (!dbName || !collName) {
        throw new Error(`Invalid namespace: ${ns}`);
    }

    // Sanity: ensure collection is sharded on {_id: 1}
    const configDB = db.getSiblingDB("config");
    const collInfo = configDB.collections.findOne({ _id: ns });
    if (!collInfo) {
        throw new Error(`Collection ${ns} is not sharded (no entry in config.collections).`);
    }

    const keyFields = Object.keys(collInfo.key);
    if (keyFields.length !== 1 || keyFields[0] !== shardKeyField) {
        throw new Error(
            `This script requires shard key with single field. Given key: ` +
            JSON.stringify(collInfo.key)
        );
    }

    if (collInfo.key[shardKeyField] === "hashed") {
        throw new Error("Hashed is auto-split, even with the balancer off.");
    } else if (collInfo.key[shardKeyField] !== 1) {
        throw new Error(
            `Shard key has unknown value. Given key: ` +
            JSON.stringify(collInfo.key)
        );
    }

    const shardCount = shardNames.length;
    if (shardCount === 0) {
        throw new Error("shardNames array is empty.");
    }

    print(`Using namespace ${ns} with ${shardCount} shards. (range: ${min} - ${max})`);

    // ---------------------------------------------------------------------------
    // Strategy for float _id in [0, 1]:
    // For N shards, create N chunks by splitting at N-1 evenly spaced points.
    //
    // Boundaries: 1/N, 2/N, ..., (N-1)/N
    //
    // Chunk ranges (conceptual):
    //   0: [0, 1/N)
    //   1: [1/N, 2/N)
    //   ...
    //   N-2: [(N-2)/N, (N-1)/N)
    //   N-1: [(N-1)/N, 1]
    //
    // We'll use splitAt with "middle: { _id: boundary }" and then move each
    // chunk using a "find" value that falls within each range.

    const admin = db.getSiblingDB("admin");

    // Create N-1 splits at evenly spaced boundaries
    for (let i = 1; i < shardCount; i++) {
        const boundaryValue = min + (max-min) * (i / shardCount);
        const boundary = {};
        boundary[shardKeyField] = boundaryValue;

        print(`Splitting at ${shardKeyField} = ${boundaryValue}...`);
        const splitRes = admin.runCommand({
            split: ns,
            middle: boundary
        });

        if (!splitRes.ok) {
            print(`WARNING: split at ${boundaryValue} failed: ${JSON.stringify(splitRes)}`);
        } else {
            print(`Split at ${boundaryValue} succeeded.`);
        }
    }

    // ---------------------------------------------------------------------------
    // Move one chunk to each shard.
    //
    // For shard i (0-based), the chunk's range is roughly:
    //   i == 0: _id in [0, 1/N)
    //   i > 0 and i < N-1: _id in [i/N, (i+1)/N)
    //   i == N-1: _id in [(N-1)/N, 1]
    //
    // We'll choose a "find" value in the middle of each interval, e.g.:
    //   mid = (low + high) / 2
    //
    // For the first chunk, low is 0.
    // For the last chunk, high is 1.
    // For interior chunks, low = i/N, high = (i+1)/N.

    for (let i = 0; i < shardCount; i++) {
        let low, high;

        if (i === 0) {
            low = 0.0;
            high = 1.0 / shardCount;
        } else if (i === shardCount - 1) {
            low = (i * 1.0) / shardCount;
            high = 1.0;
        } else {
            low = (i * 1.0) / shardCount;
            high = ((i + 1) * 1.0) / shardCount;
        }

        low = min + (max-min) * low;
        high = min + (max-min) * high;

        const mid = (low + high) / 2.0;

        const findDoc = {};
        findDoc[shardKeyField] = mid;

        const toShard = shardNames[i];
        print(
            `Moving chunk containing ${shardKeyField} ≈ ${mid} (range [${low}, ${high}]) to shard ${toShard}...`
        );

        const moveRes = admin.runCommand({
            moveChunk: ns,
            find: findDoc,
            to: toShard,
            _waitForDelete: true, // optional: wait for orphan cleanup
            _secondaryThrottle: true,
            writeConcern: { w: "majority", j: true },
        });

        if (!moveRes.ok) {
            print(`WARNING: moveChunk to ${toShard} failed: ${JSON.stringify(moveRes)}`);
        } else {
            print(`moveChunk to ${toShard} succeeded.`);
        }
    }

    print("Done: created one chunk per shard and moved each to its target shard.");
}
