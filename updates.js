// To stop writes cleanly, send SIGUSR2 to this script.

process.chdir(__dirname);
load("./common.js");

const versionArray = db.version().split('.').map(s => parseInt(s, 10));

const canTimeseries = versionArray[0] >= 5;
const tsCanDeleteWithoutMeta = versionArray[0] >= 7;

const canUpdateWithPipeline = versionArray[0] >= 5 || versionArray[0] == 4 && versionArray[1] == 4;

const fraction = 0.005;

function createRandomUpdate() {
    const r = Math.random();

    if (r < 0.2) {
        return {
            $set: {
                touchedByProcess: process.pid,
                updatedAt: new Date(),
            },
        };
    }

    if (r <= 0.4) {
        return {
            $set: { flag: r < 0.5 },
        };
    }

    if (r <= 0.6) {
        return {
            $set: {
                score: 1000 * Math.random(),
            },
        };
    }

    if (r <= 0.8) {
        return {
            $inc: {
                visitCount: 1,
            },
        };
    }

    return {
        $currentDate: {
            now: true,
        },
    };
}

const updatePipeline = [
    {
        $addFields: {
            randVal: { $rand: {} }
        }
    },
    {
        $addFields: {
            touchedByProcess: {
                $cond: [
                    { $lt: ["$randVal", 0.2] },
                    process.pid,
                    "$touchedByProcess"
                ]
            },
            updatedAt: {
                $cond: [
                    { $lt: ["$randVal", 0.2] },
                    "$$NOW",
                    "$updatedAt"
                ]
            },
            flag: {
                $cond: [
                    { $and: [{ $gte: ["$randVal", 0.2] }, { $lt: ["$randVal", 0.4] }] },
                    { $lt: [{ $rand: {} }, 0.5] },
                    "$flag"
                ]
            },
            score: {
                $cond: [
                    { $and: [{ $gte: ["$randVal", 0.4] }, { $lt: ["$randVal", 0.6] }] },
                    { $floor: { $multiply: [{ $rand: {} }, 1000] } },
                    "$score"
                ]
            },
            visitCount: {
                $cond: [
                    { $and: [{ $gte: ["$randVal", 0.6] }, { $lt: ["$randVal", 0.8] }] },
                    {$cond: [
                        { $eq: [ {$type: "$visitCount"}, "missing" ] },
                        1,
                        { $add: ["$visitCount", 1] },
                    ]},
                    "$visitCount"
                ]
            },
            archivedField: {
                $cond: [
                    { $gte: ["$randVal", 0.8] },
                    "$oldField",
                    "$archivedField"
                ]
            },
            oldField: {
                $cond: [
                    { $gte: ["$randVal", 0.8] },
                    "$$REMOVE",
                    "$oldField"
                ]
            },
            randVal: "$$REMOVE",
        }
    },
];

const allOldDocsCounts = {};

while (true) {
    for (const docSize of docSizes) {
        for (const useCustomID of customIDModes) {
            const collName = `${useCustomID ? "customID" : "sequentialID"}_${docSize}`;

            const startTime = new Date();

            const writes = {};

            const newDocsCount = 50_000;

            if (!allOldDocsCounts[collName]) {
                allOldDocsCounts[collName] = db[collName].estimatedDocumentCount();
            }

            const oldDocsCount = allOldDocsCounts[collName];

            const newDocs = Array.from(
                { length: newDocsCount },
                _ => ({
                    _id: useCustomID ? Math.random() : null,
                    rand: Math.random(),
                    str: "y".repeat(docSize),
                    fromUpdates: true,
                }),
            );

            console.log(`${collName}: Inserting ${newDocsCount.toLocaleString()} documents …`);

            try {
                const res = db[collName].insertMany(
                    newDocs,
                    {
                        ordered: false,
                    },
                );

                writes.plainInserts = Object.keys(res.insertedIds).length;
            } catch(e) {
                console.warn(`Failed to insert: ${e}`);
                sleep(3000);
            }

            try {
                if (canUpdateWithPipeline) {
                    const targetCount = newDocsCount;

                    console.log(`${collName}: Updating ${targetCount.toLocaleString()} random early documents …`);

                    /*
                    db[collName].updateMany(
                        { $sampleRate: fraction },
                        updatePipeline,
                    );
                    */

                    db[collName].aggregate(
                        [
                            {$match: {$sampleRate: 0.01}},
                            {$limit: targetCount},
                            ...updatePipeline,
                            {$merge: {
                                into: collName,
                                on: "_id",
                                whenMatched: "replace",
                                whenNotMatched: "insert",
                            }}
                        ]
                    )
                } else {
                    console.log(`${collName}: Fetching ${newDocsCount} random document IDs …`);

                    const docs = db[collName].aggregate([
                        { $sample: { size: newDocsCount }},
                        { $project: { _id: 1} },
                    ]).toArray();

                    const ids = docs.map(d => d._id);

                    console.log(`${collName}: Updating those randomly …`);

                    db.runCommand({
                        update: collName,
                        updates: ids.map( id => ({
                            q: {_id: id},
                            u: createRandomUpdate(),
                        })),
                        writeConcern: {
                            w: "majority",
                            j: true,
                        },
                    });
                }
            } catch(e) {
                console.warn(`Failed to update: ${e}`);
                sleep(3000);
            }

            writes.plainDeletes = 0;

            while (true) {
                const excess = db[collName].estimatedDocumentCount() - oldDocsCount;

                if (excess < 1) {
                    break;
                }

                try {
                    if (canUpdateWithPipeline) {
                        console.log(`${collName}: Deleting about ${newDocsCount.toLocaleString()} random documents …`);

                        writes.plainDeletes += db[collName].deleteMany(
                            { $sampleRate: 0.0001 },
                        ).deletedCount;
                    } else {
                        console.log(`${collName}: Fetching ${newDocsCount.toLocaleString()} random document IDs …`);
                        const ids = db[collName].aggregate([
                            { $sample: { size: newDocsCount } },
                            { $project: {_id: 1} },
                        ]).toArray().map(d => d._id);

                        console.log(`${collName}: Deleting those ${ids.length.toLocaleString()} documents …`);
                        writes.plainDeletes += db[collName].deleteMany(
                            {_id: {$in: ids}},
                        ).deletedCount;
                    }
                    
                    /*
                    if (canUpdateWithPipeline) {
                        console.log(`${collName}: Deleting about ${(fraction * collDocsCount).toLocaleString()} random documents …`);

                        writes.plainDeletes += db[collName].deleteMany(
                            { $sampleRate: 0.0001 },
                        ).deletedCount;
                    } else {
                        console.log(`${collName}: Fetching ${newDocsCount} random document IDs …`);
                        const docs = db[collName].aggregate([
                            { $sample: { size: newDocsCount }},
                            { $project: { _id: 1} },
                        ]).toArray();

                        const ids = docs.map(d => d._id);

                        console.log(`${collName}: Deleting those …`);

                        writes.plainDeletes += db[collName].deleteMany(
                            { _id: {$in: ids} },
                        ).deletedCount;
                    }
                        */
                } catch(e) {
                    console.warn(`Failed to delete: ${e}`);
                    break;
                }
            }

            const elapsedSecs = ((new Date()) - startTime) / 1000;
            const roundedSecs = Math.round(elapsedSecs * 100) / 100

            console.log(`${collName}: Writes sent over ${roundedSecs} secs: ${JSON.stringify(writes)}`);
        }
    }
}
