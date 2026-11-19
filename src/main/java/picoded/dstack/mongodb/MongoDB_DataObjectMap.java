package picoded.dstack.mongodb;

// Java imports
import java.util.Collection;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

// JavaCommons imports
import picoded.core.conv.ConvertJSON;
import picoded.core.conv.GenericConvert;
import picoded.core.conv.NestedObjectFetch;
import picoded.core.conv.NestedObjectUtil;
import picoded.core.conv.StringEscape;
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
import com.mongodb.client.result.DeleteResult;
import com.mongodb.MongoCommandException;

/**
 * ## Purpose
 * Support MongoDB implementation of DataObjectMap data structure.
 *
 * Built ontop of the Core_DataObjectMap implementation.
 * 
 * ## Dev Notes
 * Developers of this class would need to reference the following in MongoDB
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
	MongoCollection<Document> collection = null;
	
	/** Secondary connection, and its applicable mode */
	MongoCollection<Document> sec_collection = null;
	String sec_mode = null;
	
	/**
	 * Constructor, with name constructor
	 * 
	 * @param  inStack   hazelcast stack to use
	 * @param  name      of data object map to use
	 */
	public MongoDB_DataObjectMap(MongoDBStack inStack, String name) {
		super();
		collection = inStack.db_conn.getCollection(name);
		
		// Get the secondary collection if applicable
		if (inStack.sec_db_conn != null) {
			sec_collection = inStack.sec_db_conn.getCollection(name);
			sec_mode = inStack.sec_mode;
		}
	}
	
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
		IndexOptions opt = new IndexOptions();
		opt = opt.unique(true);
		opt = opt.name("_oid");
		
		// Due to the need for _oid to ensure consistency, we would not be creating it in the background
		// opt = opt.background(true);
		
		// Lets create the index
		collection.createIndex(Indexes.ascending("_oid"), opt);
		
		//
		// Wildcard indexing
		//
		// This helps improve general performance for arbitary data
		// at a huge cost of write performance. Useful for general purpose DataObjectMap
		// but not useful when fine hand-tunning is requried for some use cases
		//
		if (configMap.getBoolean("setupWildcardIndex", true)) {
			opt = new IndexOptions();
			opt = opt.name("wildcard");
			opt = opt.background(true);
			collection.createIndex(Indexes.ascending("$**"), opt);
		}
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

	@Override
	public long DataObjectRemoteDataMap_removeBulk(Collection<String> oids) {
		if (oids == null || oids.isEmpty()) {
			return 0;
		}
		DeleteResult res = collection.deleteMany(Filters.in("_oid", oids));
		return res.getDeletedCount();
	}
	
	/**
	 * Gets the complete remote data map, for DataObject.
	 * @return null if not exists, else a map with the data
	 **/
	public Map<String, Object> DataObjectRemoteDataMap_get(String _oid) {
		// Get the find result
		FindIterable<Document> res = collection.find(Filters.eq("_oid", _oid));
		
		// Get the Document object
		Document resObj = res.first();
		if (resObj == null) {
			return null;
		}
		
		// The return object
		Map<String, Object> ret = new HashMap<>();
		
		// Lets iterate through the object
		Set<String> fullKeys = resObj.keySet();
		for (String key : fullKeys) {
			// Skip the _id reserved col
			if (key.equalsIgnoreCase("_id")) {
				continue;
			}
			
			// Get the value
			Object val = resObj.get(key);
			
			// Unwrap the binary type
			if (val instanceof Binary) {
				val = ((Binary) val).getData();
			}
			
			// Populate the ret map
			ret.put(key, val);
		}
		
		// Neither of this works to resolve the mssqlOuterBracket issue
		// Looks like we need to properly reimplement the query support
		//
		// NestedObjectUtil.repackFullyQualifiedNameKeys(ret);
		// NestedObjectUtil.unpackFullyQualifiedNameKeys(ret);
		
		// Return the full map
		return ret;
	}
	
	/**
	 * Updates the actual backend storage of DataObject
	 * either partially (if supported / used), or completely
	 **/
	public void DataObjectRemoteDataMap_update(String _oid, Map<String, Object> fullMap,
		Set<String> updateKeys) {

		
		// Configure this to be an "upsert" query
		FindOneAndUpdateOptions opt = new FindOneAndUpdateOptions();
		opt.upsert(true);
		
		// Generate the document of changes
		// See: https://www.mongodb.com/docs/manual/reference/operator/update/setOnInsert/
		//
		// We do this via the various, set/unset/etc operators
		Document set_doc = new Document();
		Document setOnInsert_doc = new Document();
		Document unset_doc = new Document();
		
		// Lets iterate the keys, and decide accordingly
		Set<String> fullKeys = new HashSet<String>(fullMap.keySet());
		fullKeys.addAll(updateKeys);

		for (String key : fullKeys) {
			// Get the value
			Object value = fullMap.get(key);
			
			// Special _oid handling
			if (key.equals("_oid")) {
				setOnInsert_doc.append("_oid", _oid);
				continue;
			}
			
			// Wrap the binary values for the BSON format 
			// if needed
			if (value instanceof byte[]) {
				value = new Binary((byte[]) value);
			}
			
			// If value is an array, cast it to the list, as mongoDB has issues with arrays
			if (value != null && value.getClass().isArray()) {
				value = Arrays.asList(value);
			}
			
			// If value is a Map, we recast it to avoid referencing and formatting issues
			// this also resolves the issue when a Map, contains an array
			if (value instanceof Map) {
				value = ConvertJSON.toMap(ConvertJSON.fromObject(value));
			}
			
			// Lets apply the update values
			if (updateKeys.contains(key)) {
				// Handle NULL values unset
				if (value == null || value == ObjectToken.NULL) {

					unset_doc.append(key, "");
					continue;

				}
				
				// Handle values update
				set_doc.append(key, value);
				continue;
			}
			
			// OK - this is not in the update dataset
			// meaning we should do a "setOnInsert" if its not null
			if (value == null || value == ObjectToken.NULL) {
				// does nothing
			} else {
				setOnInsert_doc.append(key, value);
			}
		}

		
		// Generate the "update" doc
		Document updateDoc = new Document();
		updateDoc.append("$set", set_doc);
		updateDoc.append("$setOnInsert", setOnInsert_doc);
		updateDoc.append("$unset", unset_doc);
		
		// Upsert the document
		collection.findOneAndUpdate(Filters.eq("_oid", _oid), updateDoc, opt);
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
		DistinctIterable<String> search = collection.distinct("_oid", String.class);
		
		// Lets iterate the search
		try (MongoCursor<String> cursor = search.iterator()) {
			while (cursor.hasNext()) {
				ret.add(cursor.next());
			}
		}
		
		// Return the full keyset
		return ret;
	}
	
	//--------------------------------------------------------------------------
	//
	// Query based optimization
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Given the SQL style query, convert it into the BSON query format
	 */
	static protected Bson queryObjToBsonFilter(Query inQuery) {
		QueryType type = inQuery.type();
		
		// Handle the query according to its type
		if (inQuery.isCombinationOperator()) {
			// Lets convert each of the subquery
			List<Bson> remappedQuery = new ArrayList<>();
			for (Query subQuery : inQuery.childrenQuery()) {
				remappedQuery.add(queryObjToBsonFilter(subQuery));
			}
			// Combination type (AND, OR, NOT)
			if (type == QueryType.AND) {
				return Filters.and(remappedQuery);
			}
			if (type == QueryType.OR) {
				return Filters.or(remappedQuery);
			}
			if (type == QueryType.NOT) {
				if (remappedQuery.size() > 0) {
					throw new RuntimeException("NOT operator, expects only 1 subquery");
				}
				return Filters.not(remappedQuery.get(0));
			}
		} else {
			// Basic operator
			if (type == QueryType.EQUALS) {
				return Filters.eq(inQuery.fieldName(), inQuery.defaultArgumentValue());
			}
			if (type == QueryType.NOT_EQUALS) {
				return Filters.ne(inQuery.fieldName(), inQuery.defaultArgumentValue());
			}
			if (type == QueryType.LESS_THAN) {
				return Filters.lt(inQuery.fieldName(), inQuery.defaultArgumentValue());
			}
			if (type == QueryType.LESS_THAN_OR_EQUALS) {
				return Filters.lte(inQuery.fieldName(), inQuery.defaultArgumentValue());
			}
			if (type == QueryType.MORE_THAN) {
				return Filters.gt(inQuery.fieldName(), inQuery.defaultArgumentValue());
			}
			if (type == QueryType.MORE_THAN_OR_EQUALS) {
				return Filters.gte(inQuery.fieldName(), inQuery.defaultArgumentValue());
			}
			if (type == QueryType.LIKE) {
				// Because the LIKE operator does not natively exists,
				// we will generates its REGEX equivalent
				String val = GenericConvert.toString(inQuery.defaultArgumentValue());
				
				// Escaping special regex characters
				final String regexSpecialCharacters = ".+*?^$()[]{}|\\";
				for (int i = 0; i < regexSpecialCharacters.length(); ++i) {
					val = val.replaceAll("\\" + regexSpecialCharacters.charAt(i), "\\"
						+ regexSpecialCharacters.charAt(i));
				}
				
				// Replacing SQL syntax
				val = val.replaceAll("\\%", ".*");
				val = val.replaceAll("\\_", ".");
				
				return Filters.regex(inQuery.fieldName(), "^" + val + "$");
			}
		}
		
		throw new RuntimeException("Unkown query type : " + inQuery.type());
	}
	
	/**
	 * Performs a search query, and returns the respective DataObject keys.
	 *
	 * This is the GUID key varient of query, this is critical for stack lookup
	 *
	 * @param   queryClause, of where query statement and value
	 * @param   orderByStr string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max, use -1 to ignore
	 *
	 * @return  The String[] array
	 **/
	public String[] query_id(Query queryClause, String orderByStr, int offset, int limit) {
		// The query filter to use, use "all" if queryClause is null
		Bson bsonFilter = null;
		if (queryClause != null) {
			// Lets convert the SQL where clause to bsonFilter
			bsonFilter = queryObjToBsonFilter(queryClause);
		} else {
			// our equivalent of all filter
			bsonFilter = Filters.exists("_oid", true);
		}
		
		// Lets fetch the data, for the various _oid
		FindIterable<Document> search = null;
		
		// Chose the respective serach mode
		if (sec_mode == null) {
			// NULL safety
			search = collection.find(bsonFilter);
		} else if (sec_mode.equals("LIKE")) {
			// Use secondary connection for LIKE query
			if (queryClause != null && queryClause.toSqlString().toUpperCase().indexOf("LIKE") > 0) {
				search = sec_collection.find(bsonFilter);
			}
		} else if (sec_mode.equals("LIKEORALL")) {
			// Use secondary connection for LIKE query
			if (queryClause == null || queryClause.toSqlString().toUpperCase().indexOf("LIKE") > 0) {
				search = sec_collection.find(bsonFilter);
			}
		} else if (sec_mode.equals("QUERY")) {
			// Use secondary for all queries
			search = sec_collection.find(bsonFilter);
		}
		
		// Fallback to main collection query
		if (search == null) {
			search = collection.find(bsonFilter);
		}
		
		return executeFindIterable_id(search, orderByStr, offset, limit);
	}

	/**
	 * Performs a search query for a property matching any value in the given collection,
	 * using MongoDB's native $in filter, with optional additional WHERE clause.
	 *
	 * @param   propertyName property name to match
	 * @param   values collection of values (at most MAX_QUERY_IN_SIZE)
	 * @param   additionalWhereQuery optional additional WHERE query string (e.g. "status = ?"), or null
	 * @param   additionalWhereArgs arguments for additionalWhereQuery, or null
	 * @param   orderByStr query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   limit number of objects to return max, use -1 to ignore
	 *
	 * @return  The String[] array of _oid keys
	 **/
	@Override
	public String[] queryIn_id(String propertyName, Collection<?> values, String additionalWhereQuery,
		Object[] additionalWhereArgs, String orderByStr, int offset, int limit) {
		if (propertyName == null || propertyName.trim().length() == 0) {
			throw new IllegalArgumentException("propertyName cannot be null or empty");
		}
		if (values == null || values.isEmpty()) {
			return new String[0];
		}
		if (values.size() > MAX_QUERY_IN_SIZE) {
			throw new IllegalArgumentException("queryIn exceeds maximum allowed values: " + values.size() + " > " + MAX_QUERY_IN_SIZE);
		}

		Bson inFilter = Filters.in(propertyName, values);
		Bson activeFilter = inFilter;
		boolean isLike = false;

		if (additionalWhereQuery != null && additionalWhereQuery.trim().length() > 0) {
			Query additionalQuery = Query.build(additionalWhereQuery, additionalWhereArgs);
			if (additionalQuery != null) {
				Bson additionalFilter = queryObjToBsonFilter(additionalQuery);
				if (additionalFilter != null) {
					activeFilter = Filters.and(inFilter, additionalFilter);
				}
				if (additionalQuery.toSqlString().toUpperCase().indexOf("LIKE") > 0) {
					isLike = true;
				}
			}
		}

		FindIterable<Document> search = null;

		if (sec_mode == null) {
			search = collection.find(activeFilter);
		} else if (sec_mode.equals("LIKE")) {
			if (isLike) {
				search = sec_collection.find(activeFilter);
			}
		} else if (sec_mode.equals("LIKEORALL")) {
			if (isLike) {
				search = sec_collection.find(activeFilter);
			}
		} else if (sec_mode.equals("QUERY")) {
			search = sec_collection.find(activeFilter);
		}

		if (search == null) {
			search = collection.find(activeFilter);
		}

		return executeFindIterable_id(search, orderByStr, offset, limit);
	}

	/**
	 * Projects _oid, sorts, paginates, and returns array of _oid keys from the MongoDB search iterable.
	 *
	 * @param   search FindIterable of Document to execute
	 * @param   orderByStr string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   limit number of objects to return max, use -1 to ignore
	 *
	 * @return  The String[] array of _oid keys
	 **/
	protected String[] executeFindIterable_id(FindIterable<Document> search, String orderByStr, int offset, int limit) {
		
		// Apply the projection, to only fetch _oid
		search = search.projection(Projections.include("_oid"));
		
		// Build the orderBy clause
		if (orderByStr != null && orderByStr.length() > 0) {
			// The final sorting BSON
			Document sortBson = new Document();
			
			// Split it accordingly
			String[] orderSeq = orderByStr.split(",");
			for (int i = 0; i < orderSeq.length; ++i) {
				String subSeq = orderSeq[i];
				String subSeq_uc = subSeq.toUpperCase();
				
				// Append the order by rule accordingly
				if (subSeq_uc.endsWith("ASC")) {
					sortBson.append(subSeq.substring(0, subSeq.length() - 3).trim(), 1);
				} else if (subSeq.endsWith("DESC")) {
					sortBson.append(subSeq.substring(0, subSeq.length() - 4).trim(), -1);
				} else {
					// DEFAULT is ASC
					sortBson.append(subSeq.trim(), 1);
				}
			}
			
			// Apply the sorting
			search.sort(sortBson);
		}
		
		// Handle offset
		if (offset > 0) {
			search.skip(offset);
		}
		
		// And length
		if (limit >= 0) {
			search.limit(limit);
		}
		
		// The return list
		List<String> ret = new ArrayList<String>();
		
		// Lets iterate the search
		try (MongoCursor<Document> cursor = search.iterator()) {
			while (cursor.hasNext()) {
				ret.add(cursor.next().getString("_oid"));
			}
		}
		
		// Return the full keyset
		return GenericConvert.toStringArray(ret);
	}
	
	/**
	 * Performs a search query, and returns the respective DataObjects
	 *
	 * @param   where query statement
	 * @param   where clause values array
	 *
	 * @returns  The total count for the query
	 */
	@Override
	public long queryCount(String whereClause, Object[] whereValues) {
		// return collection count, if there is no where clause
		if (whereClause == null) {
			return collection.countDocuments();
		}
		
		// The query filter to use, use "all" if queryClause is null
		Bson bsonFilter = queryObjToBsonFilter(Query.build(whereClause, whereValues));
		
		// Perform the query and count
		return collection.countDocuments(bsonFilter);
	}
	
	//--------------------------------------------------------------------------
	//
	// Random object and loose iteration support
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Gets and return a random object ID
	 *
	 * @return  Random object ID
	 **/
	public String randomObjectID() {
		// Aggregation sample
		Document doc = collection.aggregate(Arrays.asList(Aggregates.sample(1))).first();
		if (doc != null) {
			return doc.getString("_oid");
		}
		return null;
	}
	
	/**
	 * Gets and return the next object ID key for iteration given the current ID,
	 * null gets the first object in iteration.
	 *
	 * It is important to note actual iteration sequence is implementation dependent.
	 * And does not gurantee that newly added objects, after the iteration started,
	 * will be part of the chain of results.
	 *
	 * Similarly if the currentID was removed midway during iteration, the return
	 * result is not properly defined, and can either be null, or the closest object matched
	 * or even a random object.
	 *
	 * It is however guranteed, if no changes / writes occurs. A complete iteration
	 * will iterate all existing objects.
	 *
	 * The larger intention of this function, is to allow a background thread to slowly
	 * iterate across all objects, eventually. With an acceptable margin of loss on,
	 * recently created/edited object. As these objects will eventually be iterated in
	 * repeated rounds on subsequent calls.
	 *
	 * Due to its roughly random nature in production (with concurrent objects generated)
	 * and its iterative nature as an eventuality. The phrase looselyIterate was chosen,
	 * to properly reflect its nature.
	 *
	 * Another way to phrase it, in worse case scenerio, its completely random, eventually iterating all objects
	 * In best case scenerio, it does proper iteration as per normal.
	 *
	 * @param   Current object ID, can be NULL
	 *
	 * @return  Next object ID, if found
	 **/
	public String looselyIterateObjectID(String currentID) {
		// The query filter to use, use "all" if queryClause is null
		Bson bsonFilter = null;
		if (currentID == null) {
			// our equivalent of all filter
			bsonFilter = Filters.exists("_oid", true);
		} else {
			// greater then
			bsonFilter = Filters.gt("_oid", currentID);
		}
		
		// Lets fetch the data, for the various _oid
		FindIterable<Document> search = collection.find(bsonFilter);
		search = search.projection(Projections.include("_oid"));
		
		// The final sorting BSON
		Document sortBson = new Document();
		sortBson.append("_oid", 1);
		
		// Apply the sorting
		search.sort(sortBson);
		
		// Apply the limit of 1
		search.limit(1);
		
		// Get the Document object
		Document resObj = search.first();
		if (resObj == null) {
			return null;
		}
		
		// Return the next _oid
		return resObj.getString("_oid");
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

	//--------------------------------------------------------------------------
	//
	// Index Management Implementation
	//
	//--------------------------------------------------------------------------

	private static final Logger LOGGER = Logger.getLogger(MongoDB_DataObjectMap.class.getName());

	/**
	 * Indicates whether this MongoDB DataObjectMap backend supports index management operations.
	 *
	 * @return true
	 */
	@Override
	public boolean supportsIndexManagement() {
		return true;
	}

	/**
	 * Lists all existing indexes on this MongoDB collection as normalized IndexDefinition instances.
	 *
	 * @return list of IndexDefinition objects representing existing indexes
	 */
	@Override
	public List<IndexDefinition> listIndexes() {
		List<IndexDefinition> resultList = new ArrayList<>();
		try {
			for (Document doc : collection.listIndexes()) {
				String idxName = doc.getString("name");

				// Extract and normalize keys document
				Map<String, String> keysMap = new LinkedHashMap<>();
				Document keyDoc = (Document) doc.get("key");
				if (keyDoc != null) {
					for (Map.Entry<String, Object> entry : keyDoc.entrySet()) {
						Object val = entry.getValue();
						if (val == null) {
							LOGGER.log(Level.WARNING, "Null key direction/type for field '" + entry.getKey() + "' in index '" + idxName + "' on collection: " + collection.getNamespace().getCollectionName());
							continue;
						}
						keysMap.put(entry.getKey(), String.valueOf(val));
					}
				} else {
					LOGGER.log(Level.WARNING, "Index '" + idxName + "' has null 'key' specification document on collection: " + collection.getNamespace().getCollectionName());
				}

				boolean unique = doc.getBoolean("unique", false);
				boolean sparse = doc.getBoolean("sparse", false);
				boolean background = doc.getBoolean("background", true);
				Long expireAfterSeconds = null;
				if (doc.containsKey("expireAfterSeconds") && doc.get("expireAfterSeconds") != null) {
					expireAfterSeconds = GenericConvert.toLong(doc.get("expireAfterSeconds"));
				}

				resultList.add(new IndexDefinition(idxName, keysMap, unique, sparse, background, expireAfterSeconds));
			}
		} catch (Exception e) {
			LOGGER.log(Level.WARNING, "Failed to list indexes for collection: " + collection.getNamespace().getCollectionName(), e);
		}
		return resultList;
	}

	/**
	 * Creates an index on this MongoDB collection using the provided IndexDefinition.
	 * Checks if an index with matching name or matching keys already exists before attempting creation.
	 *
	 * @param indexDef normalized index definition
	 */
	@Override
	public void createIndex(IndexDefinition indexDef) {
		if (indexDef == null || indexDef.getKeys() == null || indexDef.getKeys().isEmpty()) {
			throw new IllegalArgumentException("Index definition and keys cannot be null or empty");
		}

		String name = indexDef.getName();
		String collectionName = collection.getNamespace().getCollectionName();

		// Check if index already exists by name
		if (name != null && !name.trim().isEmpty() && hasIndex(name.trim())) {
			LOGGER.log(Level.INFO, "Index '" + name + "' already exists on collection '" + collectionName + "', skipping creation.");
			return;
		}

		// Check if an index with identical key composition already exists
		if (hasIndexWithKeys(indexDef)) {
			LOGGER.log(Level.INFO, "An index with matching keys already exists on collection '" + collectionName + "', skipping creation for: " + indexDef);
			return;
		}

		// Build MongoDB key specification document (preserving field insertion order)
		Document keysDoc = new Document();
		for (Map.Entry<String, String> entry : indexDef.getKeys().entrySet()) {
			String val = entry.getValue();
			// Convert direction strings to numeric 1 / -1 for MongoDB, otherwise preserve custom type (e.g. "text", "2dsphere")
			if (val != null && val.equals("1")) {
				keysDoc.append(entry.getKey(), 1);
			} else if (val != null && val.equals("-1")) {
				keysDoc.append(entry.getKey(), -1);
			} else {
				keysDoc.append(entry.getKey(), val);
			}
		}

		// Configure index options
		IndexOptions opt = new IndexOptions();
		if (name != null && !name.trim().isEmpty()) {
			opt.name(name.trim());
		}
		opt.unique(indexDef.isUnique());
		opt.sparse(indexDef.isSparse());
		opt.background(indexDef.isBackground());

		// Optional TTL expiration in seconds
		Long expireAfterSeconds = indexDef.getExpireAfterSeconds();
		if (expireAfterSeconds != null) {
			opt.expireAfter(expireAfterSeconds, TimeUnit.SECONDS);
		}

		// Execute index creation against MongoDB collection
		try {
			collection.createIndex(keysDoc, opt);
			LOGGER.log(Level.INFO, "Successfully created index '" + name + "' on collection '" + collectionName + "'.");
		} catch (MongoCommandException e) {
			LOGGER.log(Level.WARNING, "Failed creating index " + name + " on " + collectionName + ": " + e.getErrorMessage(), e);
			throw e;
		}
	}
	
}