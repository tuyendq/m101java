package com.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

/**
 * MongoDB 101 for Java Developer
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Play with MongoDB Driver for Java" );
        MongoClientOptions options = MongoClientOptions.builder().connectionsPerHost(500).build();
        MongoClient client = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));

        MongoDatabase db = client.getDatabase("m101java");
        // MongoCollection<Document> coll = db.getCollection("insertTest");

        // Document
        Document document = new Document()
                                .append("str", "Powered by MongoDB, Java, and Maven")
                                .append("int", 42)
                                .append("l", 1L)
                                .append("double", 1.1)
                                .append("b", true)
                                .append("date", new Date())
                                .append("objectId", new ObjectId())
                                .append("null", null)
                                .append("embeddedDoc", new Document("z", 0))
                                .append("list", Arrays.asList(1, 2, 3))
                                .append("languages", Arrays.asList("English", "Spanish"));
        System.out.println(document);

        // Bson document
        BsonDocument bsonDocument = new BsonDocument("str", new BsonString("This is BsonString"));
        System.out.println(bsonDocument);

        // Insert document
        MongoCollection<Document> coll = db.getCollection("insertTest");
        coll.drop();
        coll.insertOne(document);


        // Find document
        MongoCollection<Document> colFind = db.getCollection("findTest");
        colFind.drop();
        for (int i = 0; i < 10; i++) {
            colFind.insertOne(new Document("x", i));
        }
        Document firstDoc = colFind.find().first();
        System.out.println("Find one: " + firstDoc);

        List<Document> allDoc = colFind.find().into(new ArrayList<Document>());
        System.out.println("Find all with into:" + allDoc);

        System.out.println("Find all with iteration:");
        MongoCursor<Document> cursor = colFind.find().iterator();
        try {
            int count = 0;
            while (cursor.hasNext()) {
                Document curDoc = cursor.next();
                System.out.println("Document " + count + ": " + curDoc);
                count++;
            }
        } finally {
            cursor.close();
        }

        long totalDoc = colFind.count();
        System.out.println("Total document: " + totalDoc);

        // Find with filter
        System.out.println("=====Find with filter=====");
        // Bson filter = new Document("x", 1);
        // Bson filter = new Document("x", new Document("$gt", 5).append("$lt", 9));
        Bson filter = and(gt("x", 3), lt("x", 9));
        List<Document> filteredDoc = colFind.find(filter).into(new ArrayList<Document>());
        System.out.println("Filtered document: " + filteredDoc);



        // Close mongo client
        client.close();
        System.out.print("======Mongo client is closed successfully========");
    }
}
