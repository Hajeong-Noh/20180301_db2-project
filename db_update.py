import pprint

def get_db():
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client.project
    return db

if __name__ == "__main__":
    db = get_db()

db.city.find().forEach(function(data) {
    db.city.update({
        "_id": data._id,
        "properties.population": data.properties.population
    }, {
        "$set": {
            "properties.population": NumberInt(data.properties.population)
        }
    });
})

db.city.find().forEach(function(data) {
    db.city.update({
        "_id": data._id,
        "properties.admin_level": data.properties.admin_level
    }, {
        "$set": {
            "properties.admin_level": NumberInt(data.properties.admin_level)
        }
    });
})

db.city.find().forEach(function(data) {
    db.city.update({
        "_id": data._id,
        "properties.capital": data.properties.capital
    }, {
        "$set": {
            "properties.capital": NumberInt(data.properties.capital)
        }
    });
})

db.city.find().forEach(function(data) {
    db.city.update({
        "_id": data._id,
        "properties.ele": data.properties.ele
    }, {
        "$set": {
            "properties.ele": NumberInt(data.properties.ele)
        }
    });
})

db.city.find().forEach(function(data) {
    db.city.update({
        "_id": data._id,
        "properties.rank": data.properties.rank
    }, {
        "$set": {
            "properties.rank": NumberInt(data.properties.rank)
        }
    });
})

db.city.find().forEach(function(data) {
    db.city.update({
        "_id": data._id,
        "properties.postal_code": data.properties.postal_code
    }, {
        "$set": {
            "properties.postal_code": NumberInt(data.properties.postal_code)
        }
    });
})

db.city.find().forEach(function(data) {
    db.city.update({
        "_id": data._id,
        "properties.heritage": data.properties.heritage
    }, {
        "$set": {
            "properties.heritage": NumberInt(data.properties.heritage)
        }
    });
})
