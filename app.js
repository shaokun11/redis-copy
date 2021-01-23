const Redis = require("ioredis");
const redisFrom = new Redis({
  port: 6379,
  host: "",
  db: 0,
});
const redisTo = new Redis({
  port: 56468,
  host: "104.250.63.53",
  password: "",
  db: 1,
});
const sleep = (s = 1) => new Promise((r) => setTimeout(r, s * 1000));
let allKeysCount = 0;
let allKeys = [];
let transferCount = 0;
void (async function () {
  allKeysCount = await redisFrom.dbsize();
  const toSize = await redisTo.dbsize();
  console.log("from db size is " + allKeysCount, " to db size is ", toSize);
  await sleep(5);
  const stream = redisFrom.scanStream();
  stream.on("data", async (resultKeys) => {
    allKeys.push(...resultKeys);
    console.log("found keys progress ", allKeys.length / allKeysCount);
    if (allKeys.length === allKeysCount) {
      await sleep(5);
      startMigrate("----------------start migarte------------------------");
    }
  });
})();

async function startMigrate() {
  for (let i = 0; i < allKeys.length; i++) {
    if (i < transferCount) continue;
    let key = allKeys[i];
    let status = "done";
    const keyType = await redisFrom.type(key);
    switch (keyType) {
      case "hash":
        await redisTo.hmset(key, await redisFrom.hgetall(key));
        break;
      case "zset":
        const zaddContent = await redisFrom.zrange(key, 0, -1, "WITHSCORES");
        for (let i = 0; i < zaddContent.length; i = i + 2) {
          await redisTo.zadd(key, zaddContent[i + 1], zaddContent[i]);
        }
        break;
      case "set":
        await redisTo.sadd(key, await redisFrom.smembers(key));
        break;
      case "string":
        await redisTo.set(key, await redisFrom.get(key));
        break;
      case "list":
        await redisTo.ltrim(key, 0, 0);
        await redisTo.rpush(key, await redisFrom.lrange(key, 0, -1));
        break;
      default:
        status = "skip";
        break;
    }
    console.warn(
      JSON.stringify({
        key,
        type: keyType,
        action: status,
        index: transferCount,
        progress: transferCount / allKeysCount,
      })
    );
    transferCount++;
  }
}
