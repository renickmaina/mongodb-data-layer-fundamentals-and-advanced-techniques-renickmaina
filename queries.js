// queries.js

const { MongoClient } = require('mongodb');
require('dotenv').config();

const uri = process.env.MONGO_URI;
const dbName = "plp_bookstore";
const collectionName = "books";

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("✅ Connected to MongoDB");

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // --- Task 2: Basic CRUD Operations ---

    // 1. Find all books in a specific genre
    const fictionBooks = await collection.find({ genre: "Fiction" }).toArray();
    console.log("\n Fiction Books:", fictionBooks);

    // 2. Find books published after a certain year
    const recentBooks = await collection.find({ published_year: { $gt: 1950 } }).toArray();
    console.log("\n Books published after 1950:", recentBooks);

    // 3. Find books by a specific author
    const orwellBooks = await collection.find({ author: "George Orwell" }).toArray();
    console.log("\n George Orwell Books:", orwellBooks);

    // 4. Update the price of a specific book
    await collection.updateOne(
      { title: "1984" },
      { $set: { price: 12.5 } }
    );
    console.log("\n Updated price of '1984'");

    // 5. Delete a book by its title
    await collection.deleteOne({ title: "Moby Dick" });
    console.log("\n Deleted 'Moby Dick'");

    // --- Task 3: Advanced Queries ---
    const advancedQuery = await collection.find({
      in_stock: true,
      published_year: { $gt: 2010 }
    }, {
      projection: { title: 1, author: 1, price: 1, _id: 0 }
    }).toArray();
    console.log("\n In-stock books after 2010:", advancedQuery);

    // Sorting by price
    const sortedAsc = await collection.find().sort({ price: 1 }).toArray();
    console.log("\n Books sorted by price (ascending):", sortedAsc);

    const sortedDesc = await collection.find().sort({ price: -1 }).toArray();
    console.log("\n Books sorted by price (descending):", sortedDesc);

    // Pagination (5 per page, page 1)
    const page1 = await collection.find().skip(0).limit(5).toArray();
    console.log("\n Page 1 (5 books):", page1);

    // Pagination (page 2)
    const page2 = await collection.find().skip(5).limit(5).toArray();
    console.log("\n Page 2 (5 books):", page2);

    // --- Task 4: Aggregation Pipeline ---
    const avgPriceByGenre = await collection.aggregate([
      { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } }
    ]).toArray();
    console.log("\n Average price by genre:", avgPriceByGenre);

    const mostBooksByAuthor = await collection.aggregate([
      { $group: { _id: "$author", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log("\n Author with most books:", mostBooksByAuthor);

    const booksByDecade = await collection.aggregate([
      {
        $group: {
          _id: { $floor: { $divide: ["$published_year", 10] } },
          count: { $sum: 1 }
        }
      },
      {
        $project: {
          decade: { $multiply: ["$_id", 10] },
          count: 1,
          _id: 0
        }
      },
      { $sort: { decade: 1 } }
    ]).toArray();
    console.log("\n Books grouped by decade:", booksByDecade);

    // --- Task 5: Indexing ---
    await collection.createIndex({ title: 1 });
    console.log("\n Index created on title");

    await collection.createIndex({ author: 1, published_year: 1 });
    console.log(" Compound index created on author + published_year");

    const explainResult = await collection.find({ title: "1984" }).explain("executionStats");
    console.log("\n Explain plan:", explainResult.executionStats);

  } catch (err) {
    console.error(" Error:", err);
  } finally {
    await client.close();
    console.log("\n Connection closed");
  }
}

runQueries();
