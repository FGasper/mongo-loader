process.chdir(__dirname);
load("./shard.js");

const one_tib = Math.pow(2, 40);

const totalDataSize = one_tib * getShardNames().length;

if (totalDataSize === 0) {
    throw "Huh?? 0 shards??";
}

const docSizes = [500, 1000, 2000];
const customIDModes = [true, false];
const collectionSize = totalDataSize / docSizes.length / customIDModes.length;
