package picoded.dstack.mongodb;

// Test system include
import static org.junit.Assert.*;
import org.junit.*;

import picoded.dstack.*;

// MongoDB include
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.MongoClientSettings;
import com.mongodb.ConnectionString;
import org.bson.Document;

/**
 * Minimal mongoDB connectivity test.
 * Used to test assumptions used to build MongoDBStack implementation
 */
public class MongoDB_conn_test {
	
	@Test
	public void connectivityTest() {
		// Get the full_url
		String full_url = "mongodb://"+DStackTestConfig.MONGODB_HOST()+":"+DStackTestConfig.MONGODB_PORT()+"/testdb";

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

		// Configure the UPSERT option we plan to use
		FindOneAndUpdateOptions upsertOpt = new FindOneAndUpdateOptions();
		upsertOpt.upsert(true);

		// Generate the document to "insert"
		Document doc = new Document();
		doc.put("_oid", "123");
		doc.put("hello", "world");

		// Lets perform the upsert
		collection.findOneAndUpdate(Filters.eq("_oid", "123"), doc, upsertOpt);


	}

}