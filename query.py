import pprint

def get_db():
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client.project
    return db

if __name__ == "__main__":
    db = get_db()

    # group by leisure type and count them
    leisure_type = db.como.aggregate([
        {"$group": {"_id": "$properties.leisure",
                    "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}])

    print("==========================================================")
    print("1. Group by leisure type and count them")
    for type in leisure_type:
        pprint.pprint(type)
    print("==========================================================")

    # group by pitch_sport and count them
    pitch_type = db.como.aggregate([
        {"$match": {"properties.sport": {"$exists": 1},
                    "properties.leisure": {"$eq": "pitch"}}},
        {"$group": {"_id": "$properties.sport",
                    "count": {"$sum":1}}},
        {"$sort": {"count": -1}}])

    print("==========================================================")
    print("2. What is pitch?")
    for type in pitch_type:
        pprint.pprint(type)
    print("==========================================================")

##    # what is pitch with out sport?
##    pitch_type = db.como.aggregate([
##        {"$match": {"properties.sport": {"$exists": 0},
##                    "properties.leisure": {"$eq": "pitch"}}}])
##
##    print("==========================================================")
##    print("3. What is pitch without sport?")
##    for type in pitch_type:
##        pprint.pprint(type)
##    print("==========================================================")

    # get the nodes that have sport feature and count them
    pitch_type = db.como.aggregate([
        {"$unwind": "$properties.sport"},
        {"$group": {"_id": "$properties.sport",
                    "count": {"$sum":1}}},
        {"$sort": {"count": -1}}])
    print("==========================================================")
    print("4. What sports do the nodes have?")
    for type in pitch_type:
        pprint.pprint(type)
    print("==========================================================")

##    # slight difference from 4th query
##    leisure_type = db.como.aggregate([
##        {"$group": {"_id": "$properties.sport",
##                    "count": {"$sum": 1}}},
##        {"$sort": {"count": -1}}])
##
##    print("==========================================================")
##    print("4-2. It makes 'None' as well")
##    for type in leisure_type:
##        pprint.pprint(type)
##    print("==========================================================")
