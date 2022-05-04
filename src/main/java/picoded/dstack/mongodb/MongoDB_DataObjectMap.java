package picoded.dstack.mongodb;

// Java imports
import java.util.HashMap;
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
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import com.mongodb.client.model.Filters;

/**
 * MongoDB implementation of DataObjectMap data structure.
 *
 * Built ontop of the Core_DataObjectMap_struct implementation.
 * 
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
		// By mongodb default we ONLY index the _id (which is used as _oid)
		//
		// Overtime, when "maintainance" is called, additional
		// indexes would be generated, to improve overall query peformance.
		//
		// Assuming "autoIndex" is enabled. The default behaviour is "true"
		// As of now, nothing needs to be done, as the default index is auto created
		//
		// collection.createIndex("something")
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
		// Check if auto indexing is enabled
		if (configMap.getBoolean("autoIndex", true)) {
			// performAutoIndexing(keySet());
		}
	}
	
}