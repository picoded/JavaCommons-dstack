package picoded.dstack.mongodb;

// Test system include
import static org.junit.Assert.*;

import java.util.Map;

import org.junit.*;

import picoded.dstack.*;

// MongoDB include
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.*;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.MongoClientSettings;
import com.mongodb.ConnectionString;
import org.bson.Document;

/**
 * ## Purpose of test
 * 
 * Minimal mongoDB connectivity test.
 * Used to test assumptions used to build MongoDBStack implementation.
 * 
 * We start with the basic CRUD, then expend accordingly to our use case.
 * 
 * As much as possible we use native mongodb classes, and java base classes,
 * to avoid any unintended side effect from our library code. As such there is lots
 * of code reptition, to ensure clarity of functionality.
 */
public class MongoDB_conn_test {
	
	@Test
	public void connectivityTest() {

		//-------------------------------------
		// Connectivity Setup
		//-------------------------------------

		// Get the full_url
		String full_url = "mongodb://"+DStackTestConfig.MONGODB_HOST()+":"+DStackTestConfig.MONGODB_PORT()+"/testdb"+"?r=majority&w=majority&retryWrites=true&maxPoolSize=50";

		// Lets build using the stable API settings
		ServerApi serverApi = ServerApi.builder().version(ServerApiVersion.V1).build();
		MongoClientSettings settings = MongoClientSettings.builder()
			.applyConnectionString(new ConnectionString(full_url)).serverApi(serverApi).build();
		
		// The mongodb client
		MongoClient client = MongoClients.create(settings);
		assertNotNull(client);

		// The mongodb database
		MongoDatabase database = client.getDatabase("testdb");
		assertNotNull(database);

		// The mongodb collection
		MongoCollection<Document> collection = database.getCollection("testcollection");
		assertNotNull(collection);

		// Unique _oid index setup
		IndexOptions opt = new IndexOptions();
		opt = opt.unique(true);
		opt = opt.name("_oid");
		collection.createIndex(Indexes.ascending("_oid"), opt);
		
		//-------------------------------------
		// Data cleanup
		//-------------------------------------

		// Remove any stale data from previous test
		
		// Delete all items
		//
		// Due to the lack of an all * wildcard
		// we are using a exists OR condition, which is true
		// for all objects
		collection.deleteMany( //
			Filters.or( // 
				Filters.exists("_oid", true), //
				Filters.exists("_oid", false) //
				) //
			); //

		//-------------------------------------
		// C : Create the first document
		//-------------------------------------

		// Generate the document to first "insert"
		Document doc = new Document();
		doc.put("hello", "world");
		doc.put("_oid", "001");

		// Lets insert the first document
		collection.insertOne(doc);

		//-------------------------------------
		// R : Read the first document
		//-------------------------------------

		// Get the find result
		FindIterable<Document> findRes = collection.find(Filters.eq("_oid", "001"));
		assertNotNull( findRes );
		
		// Export the data without native mongodb '_id'
		findRes = findRes.projection(Projections.excludeId());
		
		// Get the document (as a map???)
		// in theory its possible, because a Document, is a Map
		Map<String,Object> resMap = findRes.first();
		assertNotNull( resMap );

		// Lets validate the values
		assertEquals("001", resMap.get("_oid"));
		assertEquals("world", resMap.get("hello"));
		// Ensure the native mongodb '_id' is scrubbed out
		assertEquals(null, resMap.get("_id"));

		//-------------------------------------
		// U : Update the first document
		//-------------------------------------

		// Generate the details, that needs updating
		//
		// See: https://www.mongodb.com/docs/manual/reference/operator/update/set/
		// for how to use the $set operator
		doc = new Document();
		doc.append("messsage", "the world is both big and small");
		// doc.append("_oid", "001");

		Document updateDoc = new Document();
		updateDoc.put("$set", doc);
		
		// Lets do a find and update
		collection.findOneAndUpdate(Filters.eq("_oid","001"), updateDoc);

		// Lets validate that the changes were made
		//----------------------------------------------

		// Get the find result
		findRes = collection.find(Filters.eq("_oid", "001"));
		assertNotNull( findRes );
		
		// Export the data without native mongodb '_id'
		findRes = findRes.projection(Projections.excludeId());
		
		// Get the document (as a map?)
		resMap = findRes.first();
		assertNotNull( resMap );

		// Lets validate the values
		assertEquals("001", resMap.get("_oid"));
		assertEquals("world", resMap.get("hello"));
		assertEquals(null, resMap.get("_id"));
		assertEquals("the world is both big and small", resMap.get("messsage"));


		//-------------------------------------
		// D : Delete the document !
		//-------------------------------------

		// Delete
		DeleteResult delRes = collection.deleteOne(Filters.eq("_oid", "001"));
		assertEquals(1, delRes.getDeletedCount());

		// And validate if it exists
		findRes = collection.find(Filters.eq("_oid", "001"));
		findRes = findRes.projection(Projections.excludeId());
		assertNull( findRes.first() );

	}

}