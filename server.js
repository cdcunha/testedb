//Save file primer-dataset.json from
// https://raw.githubusercontent.com/mongodb/docs-assets/primer-dataset/dataset.json
//
//Command to import file primer-dataset.json
// mongoimport --db test --collection restaurants --drop --file primer-dataset.json
var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');

var ObjectId = require('mongodb').ObjectID;

var url = 'mongodb://localhost:27017/test';

MongoClient.connect(url, function(err, db) {
  assert.equal(null, err);
  console.log("Connected correctly to server.");
  //insertDocument(db, function() {
  //findRestaurants(db, function() {
  //updateRestaurants(db, function() {
  //removeRestaurants(db, function() {
  //dropRestaurants(db, function() {
  //aggregateRestaurants(db, function() {
  indexRestaurants(db, function() {
    db.close();
  });
});

var insertDocument = function(db, callback) {
   db.collection('restaurants').insertOne( {
      "address" : {
         "street" : "2 Avenue",
         "zipcode" : "10075",
         "building" : "1480",
         "coord" : [ -73.9557413, 40.7720266 ]
      },
      "borough" : "Manhattan",
      "cuisine" : "Italian",
      "grades" : [
         {
            "date" : new Date("2014-10-01T00:00:00Z"),
            "grade" : "A",
            "score" : 11
         },
         {
            "date" : new Date("2014-01-16T00:00:00Z"),
            "grade" : "B",
            "score" : 17
         }
      ],
      "name" : "Vella",
      "restaurant_id" : "41704620"
   }, function(err, result) {
    assert.equal(err, null);
    console.log("Inserted a document into the restaurants collection.");
    callback();
  });
};

var findRestaurants = function(db, callback) {
   var cursor =db.collection('restaurants').find( );
   //var cursor =db.collection('restaurants').find( { "borough": "Manhattan" } );
   //var cursor =db.collection('restaurants').find( { "address.zipcode": "10075" } );
   //var cursor =db.collection('restaurants').find( { "grades.grade": "B" } );
   //var cursor =db.collection('restaurants').find( { "grades.score": { $gt: 30 } } );
   //var cursor =db.collection('restaurants').find( { "grades.score": { $lt: 10 } } );
   //var cursor =db.collection('restaurants').find( { "cuisine": "Italian", "address.zipcode": "10075" } );
   //var cursor =db.collection('restaurants').find( { $or: [ { "cuisine": "Italian" }, { "address.zipcode": "10075" } ] } );
   //var cursor =db.collection('restaurants').find().sort( { "borough": 1, "address.zipcode": 1 } );
   cursor.each(function(err, doc) {
      assert.equal(err, null);
      if (doc != null) {
         console.dir(doc);
      } else {
         callback();
      }
   });
};

var updateRestaurants = function(db, callback) {
   //db.collection('restaurants').updateOne(
      /*{ "name" : "Juni" },
      {
        $set: { "cuisine": "American (New)" },
        $currentDate: { "lastModified": true }
      },*/
      /*{ "restaurant_id" : "41156888" },
      { $set: { "address.street": "East 31st Street" } },*/
   /*db.collection('restaurants').updateMany(
      { "address.zipcode": "10016", cuisine: "Other" },
      {
        $set: { cuisine: "Category To Be Determined" },
        $currentDate: { "lastModified": true }
      },*/
   db.collection('restaurants').replaceOne( //After the update, the document only contains the field or fields in the replacement document.
      { "restaurant_id" : "41704620" },
      {
        "name" : "Vella 2",
        "address" : {
           "coord" : [ -73.9557413, 40.7720266 ],
           "building" : "1480",
           "street" : "2 Avenue",
           "zipcode" : "10075"
        }
      }, 
	  function(err, results) {
		  console.log(results);
		  callback();
   });
};

var removeRestaurants = function(db, callback) {
   /*db.collection('restaurants').deleteMany(
      { "borough": "Manhattan" },*/
   /*db.collection('restaurants').deleteOne(
      { "borough": "Queens" },*/
   db.collection('restaurants').deleteMany( {},  //All documents
      function(err, results) {
         console.log(results);
         callback();
      }
   );
};

var dropRestaurants = function(db, callback) {
   db.collection('restaurants').drop( function(err, response) {
      console.log(response)
      callback();
   });
};

var aggregateRestaurants = function(db, callback) {
   db.collection('restaurants').aggregate(
     [
       //{ $group: { "_id": "$borough", "count": { $sum: 1 } } } //Group by
	   { $match: { "borough": "Queens", "cuisine": "Brazilian" } },     //Filter and Group
       { $group: { "_id": "$address.zipcode" , "count": { $sum: 1 } } } //Filter and Group
     ]
   ).toArray(function(err, result) {
     assert.equal(err, null);
     console.log(result);
     callback(result);
   });
};

var indexRestaurants = function(db, callback) {
   db.collection('restaurants').createIndex(
      //{ "cuisine": 1 }, //Single-Field Index
	  { "cuisine": 1, "address.zipcode": -1 }, //compound index
      null,
      function(err, results) {
         console.log(results);
         callback();
      }
   );
};