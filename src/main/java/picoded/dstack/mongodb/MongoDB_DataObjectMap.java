package picoded.dstack.mongodb;

// Java imports
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// JavaCommons imports
import picoded.core.conv.ConvertJSON;
import picoded.core.conv.GenericConvert;
import picoded.core.conv.NestedObjectFetch;
import picoded.core.conv.StringEscape;
import picoded.core.struct.query.Query;
import picoded.core.common.ObjectToken;
import picoded.dstack.*;
import picoded.dstack.core.*;

// MongoDB imports
import org.bson.Document;
import com.mongodb.client.*;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.FindOneAndUpdateOptions;

/**
 * ## Purpose
 * Support MongoDB implementation of DataObjectMap data structure.
 *
 * Built ontop of the Core_DataObjectMap_struct implementation.
 * 
 * ## Dev Notes
 * Developers of this class would need to reference the following
 * 
 * - Collection API : https://mongodb.github.io/mongo-java-driver/4.6/apidocs/mongodb-driver-sync/com/mongodb/client/MongoCollection.html
 * - Filter API: https://mongodb.github.io/mongo-java-driver/3.6/javadoc/com/mongodb/client/model/Filters.html#where-java.lang.String-
 **/
public class MongoDB_DataObjectMap extends Core_DataObjectMap {
	
	//--------------------------------------------------------------------------
	//
	// Constructor
	//
	//--------------------------------------------------------------------------
	
	/** MongoDB instance representing the backend connection */
	MongoDBStack hazelcastStack = null;
	MongoCollection<Document> collection = null;
	
	/**
	 * Constructor, with name constructor
	 * 
	 * @param  inStack   hazelcast stack to use
	 * @param  name      of data object map to use
	 */
	public MongoDB_DataObjectMap(MongoDBStack inStack, String name) {
		super();
		collection = inStack.db_conn.getCollection(name);
	}
	
	//--------------------------------------------------------------------------
	//
	// BSON utilities
	//
	//--------------------------------------------------------------------------
	
	//--------------------------------------------------------------------------
	//
	// Backend system setup / teardown (DStackCommon)
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Setsup the backend storage table, etc. If needed
	 **/
	@Override
	public void systemSetup() {
		//
		// By mongodb default we use its native _id implementation
		// and handle our _oid seperately.
		//
		// We intentionally DO NOT use mongodb _id, allowing it retain optimal performance.
		//
		// Overtime, when "maintainance" is called, additional
		// indexes would be generated, to improve overall query peformance.
		//
		// This assumes "autoIndex" is enabled. The default behaviour is "true"
		//
		IndexOptions opt = new IndexOptions();
		opt = opt.unique(true);
		opt = opt.name("_oid");
		collection.createIndex(Indexes.ascending("_oid"), opt);
		
		// @TODO consider wildcard index
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
				Filters.exists("_oid", true), //
				Filters.exists("_oid", false) //
				) //
			); //
	}
	
	//--------------------------------------------------------------------------
	//
	// Internal functions, used by DataObject
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Removes the complete remote data map, for DataObject.
	 * This is used to nuke an entire object
	 *
	 * @param  Object ID to remove
	 *
	 * @return  nothing
	 **/
	public void DataObjectRemoteDataMap_remove(String _oid) {
		// Delete the data
		collection.deleteOne(Filters.eq("_oid", _oid));
	}
	
	/**
	 * Gets the complete remote data map, for DataObject.
	 * @return null if not exists, else a map with the data
	 **/
	public Map<String, Object> DataObjectRemoteDataMap_get(String _oid) {
		// Get the find result
		FindIterable<Document> res = collection.find(Filters.eq("_oid", _oid));
		
		// Export the data without _id
		res = res.projection(Projections.excludeId());
		
		// Return the first document (if any)
		return res.first();
	}
	
	/**
	 * Updates the actual backend storage of DataObject
	 * either partially (if supported / used), or completely
	 **/
	public void DataObjectRemoteDataMap_update(String _oid, Map<String, Object> fullMap,
		Set<String> keys) {
		
		// Configure this to be an "upsert" query
		FindOneAndUpdateOptions opt = new FindOneAndUpdateOptions();
		opt.upsert(true);
		
		// Generate the document to "upsert"
		Document doc = new Document();
		for (String key : keys) {
			doc.put(key, fullMap.get(key));
		}
		// Enforce _oid
		doc.put("_oid", _oid);
		
		// Upsert the document
		collection.findOneAndUpdate(Filters.eq("_oid", _oid), doc, opt);
	}
	
	//--------------------------------------------------------------------------
	//
	// KeySet support
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Get and returns all the GUID's, note that due to its
	 * potential of returning a large data set, production use
	 * should be avoided.
	 *
	 * @return set of keys
	 **/
	@Override
	public Set<String> keySet() {
		// The return hashset
		HashSet<String> ret = new HashSet<String>();
		
		// Lets fetch everything ... D=
		FindIterable<Document> search = collection.find();
		search = search.projection(Projections.include("_oid"));
		
		// Lets iterate the search
		try (MongoCursor<Document> cursor = search.iterator()) {
			while (cursor.hasNext()) {
				ret.add(cursor.next().getString("_oid"));
			}
		}
		
		// Return the full keyset
		return ret;
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
	
	/**
	 * Maintenance call, which performs various index setup used to optimize
	 * the JSONB setup.
	 **/
	@Override
	public void maintenance() {
		// @TODO consider wildcard index
		// // Check if auto indexing is enabled
		// if (configMap.getBoolean("autoIndex", true)) {
		// 	// performAutoIndexing(keySet());
		// }
	}
	
}