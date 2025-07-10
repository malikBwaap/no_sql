from pymongo import MongoClient

from pprint import pprint

client = MongoClient(
    host="127.0.0.1",
    port = 27017,
    username = "datascientest",
    password = "dst123"
)


print(" b -----------------------------------------------------------")
print(client.list_database_names())

db = client["sample"]

print(" c -----------------------------------------------------------")
collections = db.list_collection_names()
print(collections)

print(" d -----------------------------------------------------------")
one_book = db.books.find_one()
pprint(one_book)

print(" e -----------------------------------------------------------")
num_books = db.books.count_documents({})
print(num_books)


print(" a -----------------------------------------------------------")
# Count books with pageCount > 400
count_more_400 = db.books.count_documents({"pageCount": {"$gt": 400}})
print("Books with more than 400 pages:", count_more_400)


# Count number of books with more than 400 pages and were published
count_more_400_published = db.books.count_documents(
    {"pageCount": {"$gt": 400}, "status": "PUBLISH"}
)
print(
    "Published books with more than 400 pages:",
    count_more_400_published
)

print(" b -----------------------------------------------------------")
# number of books with the keyword Android in their description
count_android = db.books.count_documents(
    {
        "$or": [
            {"shortDescription": {"$regex": "Android", "$options": "i"}},
            {"longDescription": {"$regex": "Android", "$options": "i"}}
        ]
    }
)
print("Number of books mentioning Android:", count_android)


print(" c -----------------------------------------------------------")
# Aggregate to get distinct categories at index 0 and 1
result = list(
    db.books.aggregate(
        [
            {
                "$group": {
                    "_id": None,
                    "category0": {"$addToSet": {"$arrayElemAt": ["$categories", 0]}},
                    "category1": {"$addToSet": {"$arrayElemAt": ["$categories", 1]}}
                }
            }
        ]
    )
)

# Print the two lists
if result:
    categories_0 = result[0]["category0"]
    categories_1 = result[0]["category1"]
    print("Distinct Category1 list:", categories_0)
    print("Distinct Category2 list:", categories_1)
else:
    print("No data found")


# d number of books containing the names Python, Java, C++, etc
print(" d -------------------------------------------------------------")
count_languages = db.books.count_documents(
    {
        "$or": [
            {"longDescription": {"$regex": "Python", "$options": "i"}},
            {"longDescription": {"$regex": "Java", "$options": "i"}},
            {"longDescription": {"$regex": "C\\+\\+", "$options": "i"}},  # Escape the +
            {"longDescription": {"$regex": "Scala", "$options": "i"}}
        ]
    }
)

print("Number of books mentioning Python, Java, C++, or Scala:", count_languages)



print(" e -------------------------------------------------------------")
# Aggregation pipeline
pipeline = [
    {"$unwind": "$categories"},  # Unwind the categories array
    {
        "$group": {
            "_id": "$categories",
            "minPages": {"$min": "$pageCount"},
            "maxPages": {"$max": "$pageCount"},
            "avgPages": {"$avg": "$pageCount"}
        }
    }
]

# Run the pipeline
results = list(db.books.aggregate(pipeline))

# Print results nicely
for r in results:
    category = r["_id"]
    min_pages = r["minPages"]
    max_pages = r["maxPages"]
    avg_pages = r["avgPages"]
    print(
        f"{category}: min={min_pages}, max={max_pages}, avg={avg_pages:.2f}"
    )



print(" f -------------------------------------------------------------")
# Aggregation pipeline
pipeline = [
    # Step 1: Extract year, month, day from $publishedDate
    {
        "$addFields": {
            "pubYear": {"$year": "$publishedDate"},
            "pubMonth": {"$month": "$publishedDate"},
            "pubDay": {"$dayOfMonth": "$publishedDate"}
        }
    },
    # Step 2: Filter books published after 2009
    {
        "$match": {
            "pubYear": {"$gt": 2009}
        }
    },
    # Step 3: Limit to first 20 documents
    {
        "$limit": 20
    }
]

results = list(db.books.aggregate(pipeline))

for book in results:
    print(
        f"Title: {book.get('title', '<no title>')}, "
        f"Published Date: {book['pubYear']}-{book['pubMonth']:02d}-{book['pubDay']:02d}"
    )




print(" g -------------------------------------------------------------")
pipeline = [
    # Step 1: Sort chronologically
    {"$sort": {"dates": 1}},
    # Step 2: Project new columns from authors list
    {
        "$project": {
            "title": 1,  # Keep the title
            "author_1": {"$arrayElemAt": ["$authors", 0]},
            "author_2": {"$arrayElemAt": ["$authors", 1]},
            "author_3": {"$arrayElemAt": ["$authors", 2]},
            "author_4": {"$arrayElemAt": ["$authors", 3]},
        }
    },
    # Step 3: Limit to first 20
    {"$limit": 20}
]

# Run the pipeline
for book in db.books.aggregate(pipeline):
    print(book)



print(" h -------------------------------------------------------------")
pipeline = [
    # Step 1: Add a field for first author (authors[0])
    {
        "$addFields": {
            "first_author": {"$arrayElemAt": ["$authors", 0]}
        }
    },
    # Step 2: Group by first_author and count
    {
        "$group": {
            "_id": "$first_author",
            "num_publications": {"$sum": 1}
        }
    },
    # Step 3: Sort descending by number of publications
    {
        "$sort": {"num_publications": -1}
    },
    # Step 4: Limit to top 10 authors
    {
        "$limit": 10
    }
]

# Run the aggregation
results = list(db.books.aggregate(pipeline))

# Print results
for r in results:
    print(f"Author: {r['_id']}, Publications: {r['num_publications']}")



print(" i -------------------------------------------------------------")
pipeline = [
    # Step 1: Add a field 'num_authors' that counts the size of the authors array
    {
        "$addFields": {
            "num_authors": {"$size": "$authors"}
        }
    },
    # Step 2: Group by 'num_authors' and count how many books have this number
    {
        "$group": {
            "_id": "$num_authors",
            "count": {"$sum": 1}
        }
    },
    # Optional: Sort by number of authors ascending
    {
        "$sort": {"_id": 1}
    }
]

results = list(db.books.aggregate(pipeline))

# Display results
for r in results:
    print(f"Number of authors: {r['_id']}, Number of books: {r['count']}")



print(" j -------------------------------------------------------------")
pipeline = [
    # Unwind authors, keeping the index as "authorIndex"
    {
        "$unwind": {
            "path": "$authors",
            "includeArrayIndex": "authorIndex"
        }
    },
    # Project to keep only non-empty authors
    {
        "$project": {
            "author": "$authors",
            "authorIndex": 1
        }
    },
    # Filter out empty or null authors
    {
        "$match": {
            "author": {"$ne": None, "$ne": ""}
        }
    },
    # Group by author name and index position, count occurrences
    {
        "$group": {
            "_id": {
                "author": "$author",
                "index": "$authorIndex"
            },
            "count": {"$sum": 1}
        }
    },
    # Sort by descending count
    {
        "$sort": {"count": -1}
    },
    # Limit to top 20
    {
        "$limit": 20
    }
]

results = list(db.books.aggregate(pipeline))

# Display nicely
for r in results:
    author = r["_id"]["author"]
    index = r["_id"]["index"]
    count = r["count"]
    print(f"Author: {author}, Position: {index}, Occurrences: {count}")
