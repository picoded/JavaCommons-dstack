package picoded.dstack.mongodb;

// Java imports
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// JavaCommons imports
import picoded.core.conv.ConvertJSON;
import picoded.core.conv.GenericConvert;
import picoded.core.conv.NestedObjectFetch;
import picoded.core.conv.NestedObjectUtil;
import picoded.core.conv.StringEscape;
import picoded.core.struct.MutablePair;
import picoded.core.struct.query.OrderBy;
import picoded.core.struct.query.Query;
import picoded.core.struct.query.QueryType;
import picoded.core.common.ObjectToken;
import picoded.dstack.*;
import picoded.dstack.core.*;

// MongoDB imports
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.conversions.Bson;
import com.mongodb.client.*;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Aggregates;

/**
 * ## Purpose Support MongoDB implementation of KeyValueMap
 *
 * Built ontop of the Core_KeyValueMap implementation.
 **/
public class MongoDB_KeyValueMap extends Core_KeyValueMap {
	
	// --------------------------------------------------------------------------
	//
	// Constructor
	//
	// --------------------------------------------------------------------------
	
	/** MongoDB instance representing the backend connection */
	MongoCollection<Document> collection = null;
	
	/**
	 * Constructor, with name constructor
	 * 
	 * @param inStack hazelcast stack to use
	 * @param name    of data object map to use
	 */
	public MongoDB_KeyValueMap(MongoDBStack inStack, String name) {
		super();
		collection = inStack.db_conn.getCollection(name);
	}
	
	@Override
	public void systemSetup() {
		//
		// By mongodb default we use its native _id implementation
		// and handle our _oid seperately.
		//
		// We intentionally DO NOT use mongodb _id, allowing it retain optimal performance.
		//
		
		// Lets create the unique key index
		IndexOptions opt = new IndexOptions().unique(true).name("key");
		collection.createIndex(Indexes.ascending("key"), opt);
		
		// Expirary key support
		opt = new IndexOptions().expireAfter(0L, TimeUnit.SECONDS);
		collection.createIndex(Indexes.ascending("expireAt"), opt);
	}
	
	/**
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	public void systemDestroy() {
		collection.drop();
	}
	
	/**
	 * Removes all data, without tearing down setup
	 **/
	@Override
	public void clear() {
		// Delete all items
		//
		// Due to the lack of an all * wildcard
		// we are using a exists OR condition, which is true
		// for all objects
		collection.deleteMany( //
			Filters.or( //
				Filters.exists("key", true), //
				Filters.exists("key", false) //
				) //
			); //
	}
	
	//--------------------------------------------------------------------------
	//
	// Internal functions, used by DataObject
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Generate a BSON filter set, for unexpired items
	 * this should be used in combination with an AND clause filter
	 **/
	protected Bson filterForUnexpired() {
		// Current timestamp
		Date now = new Date();
		
		// the or array to join
		return Filters.or( //
			Filters.exists("expireAt", false), //
			Filters.gt("expireAt", now), //
			Filters.lte("expireAt", 0) //
			); //
	}
	
	/**
	 * Search using the value, all the relevent key mappings
	 *
	 * Handles re-entrant lock where applicable
	 *
	 * @param key, note that null matches ALL
	 *
	 * @return array of keys
	 **/
	@Override
	public Set<String> keySet(String value) {
		// The return hashset
		HashSet<String> ret = new HashSet<String>();
		
		// Search result
		FindIterable<Document> search = null;
		
		// Lets either fetch with a value, or everything
		if (value == null) {
			// Lets fetch everything ... D=
			search = collection.find(filterForUnexpired());
		} else {
			search = collection.find(Filters.and( //
				filterForUnexpired(), //
				Filters.eq("val", value) //
				)); //
		}
		
		// Get all the various keys
		search = search.projection(Projections.include("key"));
		
		// Lets iterate the search
		try (MongoCursor<Document> cursor = search.iterator()) {
			while (cursor.hasNext()) {
				ret.add(cursor.next().getString("key"));
			}
		}
		
		// Return the full keyset
		return ret;
	}
	
	//--------------------------------------------------------------------------
	//
	// Fundemental set/get value (core)
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * Sets the value, with validation
	 *
	 * @param key
	 * @param value, null means removal
	 * @param expire timestamp, 0 means not timestamp
	 *
	 * @return null
	 **/
	@Override
	public String setValueRaw(String key, String value, long expire) {
		// Configure this to be an "upsert" query
		FindOneAndUpdateOptions opt = new FindOneAndUpdateOptions();
		opt.upsert(true);
		
		// Generate the document of changes
		// See: https://www.mongodb.com/docs/manual/reference/operator/update/setOnInsert/

		// Generate the "update" doc
		Document updateDoc = new Document();
        Document set_doc = new Document();
		
		// Expire timestamp if its configured, else it should be removed
		if (expireAt > 0) {
			set_doc.append("expireAt", new Date(expireAt));
		} else {
			Document unset_doc = new Document();
			unset_doc.append("expireAt", "");
			updateDoc.append("$unset", unset_doc);
		}
		
        // Setup the value on update/insert/upsert
		set_doc.append("val", value);
        updateDoc.append("$set", set_doc);
		
		// Set the key on insert
		Document setOnInsert_doc = new Document();
		setOnInsert_doc.append("key", key);
		updateDoc.append("$setOnInsert", setOnInsert_doc);
		
		// Upsert the document
		collection.findOneAndUpdate(Filters.eq("key", key), updateDoc, opt);
		return null;
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * 
	 * Returns the value and expiry, with validation against the current timestamp
	 * 
	 * @param key as String
	 * @param now timestamp
	 *
	 * @return String value
	 **/
	@Override
	public MutablePair<String, Long> getValueExpiryRaw(String key, long now) {
		// Get the find result
		FindIterable<Document> res = collection.find(Filters.eq("key", key));
		
		// Get the Document object
		Document resObj = res.first();
		if (resObj == null) {
			return null;
		}
		
		// Lets get all the key values
		String val = GenericConvert.toString(resObj.get("val"), null);
		Date expireAt_date = resObj.get("expireAt");
        long expireAt_long = 0;
		
        // Check if expireAt date is set
        if( expireAt_date != null ) {
            expireAt_long = expireAt_date.getTime();
        }

		// Check for null objects
		if (val == null || val.isEmpty()) {
			return null;
		}
		
		// No valid value found, return null
		if (expireAt < 0) {
			return null;
		}
		
		// Expired value, return null
		if (expireAt != 0 && expireAt < now) {
			return null;
		}
		
		// Get the value, and return the pair
		return new MutablePair<String, Long>(val, expireAt);
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * Sets the expire time stamp value, raw without validation
	 *
	 * @param key as String
	 * @param expireAt timestamp in seconds, 0 means NO expire
	 **/
	@Override
	public void setExpiryRaw(String key, long expireAt) {
		// Configure this to be an "update" query
		FindOneAndUpdateOptions opt = new FindOneAndUpdateOptions();
		
		// Generate the document of changes
		// See: https://www.mongodb.com/docs/manual/reference/operator/update/setOnInsert/
		
		// Generate the "update" doc
		Document updateDoc = new Document();
		
		// Expire timestamp if its configured, else it should be ignored
		if (expireAt > 0) {
			Document set_doc = new Document();
			set_doc.append("expireAt", new Date(expireAt));
			updateDoc.append("$set", set_doc);
		} else {
			Document unset_doc = new Document();
			unset_doc.append("expireAt", "");
			updateDoc.append("$unset", unset_doc);
		}
		
		// Upsert the document
		collection.findOneAndUpdate(Filters.eq("key", key), updateDoc, opt);
	}
	
	//--------------------------------------------------------------------------
	//
	// Remove call
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Remove the value, given the key
	 *
	 * @param key param find the thae meta key
	 *
	 * @return  null
	 **/
	@Override
	public KeyValue remove(Object key) {
		removeValue(key);
		return null;
	}
	
	/**
	 * Remove the value, given the key
	 *
	 * Important note: It does not return the previously stored value
	 * Its return String type is to maintain consistency with Map interfaces
	 *
	 * @param key param find the thae meta key
	 *
	 * @return  null
	 **/
	@Override
	public String removeValue(Object key) {
		if (key == null) {
			throw new IllegalArgumentException("delete 'key' cannot be null");
		}
		
		// Delete the data
		collection.deleteOne(Filters.eq("key", key));
		return null;
	}
	
	//--------------------------------------------------------------------------
	//
	// Maintenance calls
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Incremental maintainance should not trigger maintenance.
	 * As its potentially blocking with a very long call
	 **/
	public void incrementalMaintenance() {
		// does nothing
	}
	
	@Override
	public void maintenance() {
		// @TODO : something? (not sure what needs to be done)
	}
	
}