import { MongoClient } from "https://deno.land/x/mongo@v0.31.2/mod.ts";

const clients = new Array(1000).fill(0);

console.time("Total time");
await Promise.all(clients.map(async (_, i) => {
  const client = new MongoClient();
  await client.connect("mongodb://localhost:27017");

  const db = client.database("test");

  const col = db.collection("test");

  const docs = new Array(1000).fill(0).map((_, j) => ({ client: i, doc: j }));

  console.log(`Client ${i} inserting docs...`);
  console.time(`Client ${i} insert time`);

  await col.insertMany(docs);

  console.log(`Client ${i} inserted docs!`);
  console.timeEnd(`Client ${i} insert time`);
}));
console.timeEnd("Total time");
