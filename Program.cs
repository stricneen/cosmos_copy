using System;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Driver;

namespace CosmosCopy
{
    class Program
    {
        static void Main(string[] args)
        {
            // 31285

            var writeConnection = "<destination>";
            var client = new MongoClient(writeConnection);
            var database  = client.GetDatabase("cases-db");
            var collection = database.GetCollection<dynamic>("trg-backup");

            var total = 31285;
            var counter = 0;
            foreach (var c in Read()) {
                try{
                collection.InsertOne(c);
                } catch (MongoWriteException ex) {
                    // expected on rerun
                }
                counter++;

                if (counter % 500 == 0) {

                    Console.Write("\r{0}% - {1}", counter / total * 100m, counter);
                }
            }


        }

        static IEnumerable<object> Read() {
            
            // Read only connection to PROD
            var connection = "<source>";

            var client = new MongoClient(connection);
            var database  = client.GetDatabase("casesdb");
            var collection = database.GetCollection<dynamic>("cases");

            var filter = Builders<BsonDocument>.Filter.Empty;
            var c = collection.CountDocuments(_ => true);
            Console.WriteLine("Document count : " + c);
            
            var cursor = collection.Find(_=> true).ToCursor();
            foreach (var document in cursor.ToEnumerable())
            {
                yield return document;
            }

        }
    }
}
