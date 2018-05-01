import pprint

def get_db():
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client.project
    return db

if __name__ == "__main__":
    db = get_db()

    # 1. Let's check if there is a city 'Como'!
    como_city = db.city.find({"properties.name": "Como"})
    print("==========================================================")
    print("1. Let's check if there is a city 'Como'!")
    for city in como_city:
        pprint.pprint(city)
    print("==========================================================")

    # 2. Are there cities that have same admin_level and capital with Como?
    cities = db.city.find({"properties.admin_level": 8, "properties.capital": 6})
    print("==========================================================")
    print("2. Are there cities that have same admin_level and capital with Como?")
    for city in cities:
        pprint.pprint(city)
    print("==========================================================")

    # 3. Can we make the result looks simpler?
    query = {"properties.admin_level": 8, "properties.capital": 6}
    projection = {"_id": 0, "properties.name": 1}
    cities = db.city.find(query, projection)
    print("==========================================================")
    print("3. Can we make the result looks simpler?")
    for city in cities:
        pprint.pprint(city)
    print("==========================================================")

    # 4. How many cities are in the database?
    num_cities = db.city.find().count()
    print("==========================================================")
    print("4. How many cities are in the database?")
    print("The amount of total Italian cities is " + str(num_cities))
    print("==========================================================")

    # 5-1. Which cities has the postal code starts with 2?
    query = {"properties.postal_code":{"$gte": 20000, "$lt": 30000}}
    projection = {"_id": 0, "properties.name": 1, "properties.postal_code": 2}
    cities = db.city.find(query, projection)
    print("==========================================================")
    print("5-1. Which cities has the postal code starts with 2?")
    for city in cities:
        pprint.pprint(city)
    print("==========================================================")

    # 5-2. Which cities start with A?
    query = {"properties.name":{"$gte": "A", "$lt": "B"}}
    projection = {"_id": 0, "properties.name": 1}
    cities = db.city.find(query, projection)
    print("==========================================================")
    print("5-2. Which cities start with A?")
    for city in cities:
        pprint.pprint(city)
    print("==========================================================")

    # 6. Which cities are registered by Unesco Heritage Centre
    query = {"properties.heritage":{"$exists": 1}}
    projection = {"_id": 0, "properties.name": 1}
    cities = db.city.find(query, projection)
    print("==========================================================")
    print("6. Which cities are registered by Unesco Heritage Centre")
    for city in cities:
        pprint.pprint(city)
    print("==========================================================")
   
    # 7. What is the relation between capital and population?
    cities = db.city.aggregate([
        {"$group": {"_id": "$properties.capital",
                    "count": {"$sum": 1},
                    "average_population": {"$avg": "$properties.population"}}},
        {"$sort": {"average_population": -1}}])

    print("==========================================================")
    print("7. What is the relation between capital and population?")
    for city in cities:
        pprint.pprint(city)
    print("==========================================================")

    # 8. What is the relation between elevation and population?
    cities = db.city.aggregate([
        {"$match": {"properties.ele": {"$exists": 1}}},
        {"$group": {"_id": "$properties.ele",
                    "count": {"$sum":1},
                    "average_population": {"$avg": "$properties.population"}}},
        {"$sort": {"average_population": -1}}])

    print("==========================================================")
    print("8. What is the relation between elevation and population?")
    for city in cities:
        pprint.pprint(city)
    print("==========================================================")

    # 9. What is the proportion of heritage cities out of each capital category?
    cities = db.city.aggregate([
        {"$match": {"properties.capital": {"$exists": 1}}},
        {"$group": {"_id": "$properties.capital",
                    "count": {"$sum":1},
                    "num_heritage": {"$sum": "$properties.heritage"}}},
        {"$project": {"ratio": {"$divide": ["$num_heritage", "$count"]}}},
        {"$sort": {"_id": 1}}])
    
    print("==========================================================")
    print("9. What is the proportion of heritage cities out of each capital category?")
    for city in cities:
        pprint.pprint(city)
    print("==========================================================")

    # 10-1. How many different ranks does each capital categories have?
    cities = db.city.aggregate([
        {"$match": {"properties.capital": {"$exists": 1}}},
        {"$group": {"_id": "$properties.capital",
                    "rank_set": {
                        "$addToSet": "$properties.rank"
                    }}},
        {"$sort": {"_id": 1}}])

    print("==========================================================")
    print("10-1. How many different ranks does each capital categories have?")
    for city in cities:
        pprint.pprint(city)
    print("==========================================================")

    # 10-2. How many different ranks does each capital categories have?
    cities = db.city.aggregate([
        {"$match": {"properties.capital": {"$exists": 1}}},
        {"$group": {"_id": "$properties.capital",
                    "rank_set": {
                        "$addToSet": "$properties.rank"
                    }}},
        {"$unwind": "$rank_set"},
        {"$sort": {"_id": 1}}])

    print("==========================================================")
    print("10-2. How many different ranks does each capital categories have?")
    for city in cities:
        pprint.pprint(city)

    # 10-3. How many different ranks does each capital categories have?
    cities = db.city.aggregate([
        {"$match": {"properties.capital": {"$exists": 1}}},
        {"$group": {"_id": "$properties.capital",
                    "rank_set": {
                        "$addToSet": "$properties.rank"
                    }}},
        {"$unwind": "$rank_set"},
        {"$group": {"_id": "$_id", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}])

    print("==========================================================")
    print("10-3. How many different ranks does each capital categories have?")
    for city in cities:
        pprint.pprint(city)
    print("==========================================================")


