const stream = require("stream");
const ndjson = require("ndjson");
const through2 = require("through2");
const request = require("request");
const filter = require("through2-filter");
const sentiment = require("sentiment");
const util = require("util");
const pipeline = util.promisify(stream.pipeline);


const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');

const CONNECTION_URL = 'mongodb://localhost:27017';

// Database Name
const dbName = 'hacker-news_pipeline';

(async () => {
  const client = new MongoClient(CONNECTION_URL);
  const textRank = new sentiment();
  try {
      await client.connect();
      const collection = client.db(dbName).collection("mentions");
      
      await pipeline(
        request("https://api.hnstream.com/comments/stream/"),
        ndjson.parse({strict: false}),
        filter({ objectMode: true }, chunk => {
          console.log(chunk["article-title"].toLowerCase().includes("Licences"), 'First one');
          console.log(chunk["body"].toLowerCase().includes("Licences"), '2nd one');
          return chunk["body"].toLowerCase().includes("Licences") || chunk["article-title"].toLowerCase().includes("Licences");
        }),
        through2.obj((row, enc, next) => {
          console.log(row.body, 'Rows body message')
          let result = textRank.analyze(row.body);
          row.score = result.score;
          next(null, row);
        }),
        through2.obj((row, enc, next) => {
            console.log(row, 'Hello');
            collection.insertOne({
                ...row,
                "user-url": `https://news.ycombinator.com/user?id=${row["author"]}`,
                "item-url": `https://news.ycombinator.com/item?id=${row["article-id"]}`
            });
            next();
        })
      );
      console.log("Completed")
      console.log("FINISHED");
  } catch(error) {
      console.log(error);
  }
})();