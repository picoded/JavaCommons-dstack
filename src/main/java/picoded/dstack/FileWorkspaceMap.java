package picoded.dstack;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import picoded.core.struct.template.UnsupportedDefaultMap;

/**
 * FileWorkspaceMap, serves as the File focused flexible backend storage
 * implmentation for DStack.
 *
 * Its role can be view primarily as a flexible interface to object based storage system
 *
 * @TODO : Future consideration - integration as a FileSystemProvider : https://docs.oracle.com/javase/7/docs/api/java/nio/file/spi/FileSystemProvider.html#getScheme()
 **/
public interface FileWorkspaceMap extends UnsupportedDefaultMap<String, FileWorkspace>,
	CommonStructure {
	
	// FileWorkspaceMap optimizations
	//--------------------------------------------------------------------------
	
	/**
	 * Does a simple, get and check for null (taking advantage of the null is delete feature)
	 *
	 * Default implementation used keySet, which has extremly high performance
	 * penalties in deployments with large scale deployment
	 **/
	@Override
	default boolean containsKey(Object key) {
		return get(key) != null;
	}
	
	// FileWorkspaceMap operations
	//--------------------------------------------------------------------------
	
	/**
	 * Generates a new blank workspace, with a GUID.
	 * Note that save does not trigger, until the first write event occurs.
	 *
	 * @return the DataObject
	 **/
	FileWorkspace newEntry();
	
	/**
	 * Get a FileWorkspace, and returns it.
	 *
	 * Existance checks is performed for such requests
	 *
	 * @param  object GUID to fetch
	 *
	 * @return the DataObject, null if not exists
	 **/
	FileWorkspace get(Object oid);
	
	/**
	 * Get a FileWorkspace, and returns it. Skips existance checks if required
	 *
	 * @param  object GUID to fetch
	 * @param  boolean used to indicate if an existance check is done for the request
	 *
	 * @return the DataObject
	 **/
	FileWorkspace get(String oid, boolean isUnchecked);
	
	/**
	 * Removes a DataObject if it exists, from the DB
	 *
	 * @param  object GUID to fetch
	 *
	 * @return NULL
	 **/
	FileWorkspace remove(Object key);
	
	/**
	 * Setup the current fileWorkspace within the fileWorkspaceMap,
	 *
	 * This ensures the workspace _oid is registered within the map,
	 * even if there is 0 files.
	 *
	 * Does not throw any error if workspace was previously setup
	 */
	void setupWorkspace(String oid, String folderPath);
	
	// FileWorkspaceMap utility operations
	//--------------------------------------------------------------------------
	
	/**
	 * Get array of DataObjects
	 **/
	default FileWorkspace[] getArrayFromID(String[] idArray, boolean isUnchecked) {
		FileWorkspace[] retArr = new FileWorkspace[idArray.length];
		for (int i = 0; i < idArray.length; ++i) {
			retArr[i] = get(idArray[i], isUnchecked);
		}
		return retArr;
	}
	
	// Resolving class inheritence conflict
	//--------------------------------------------------------------------------
	
	/**
	 * Removes all data, without tearing down setup
	 *
	 * This is equivalent of "TRUNCATE TABLE {TABLENAME}"
	 **/
	void clear();
	
	// Special iteration support
	//--------------------------------------------------------------------------
	
	/**
	 * Gets and return a random object ID
	 *
	 * @return  Random object ID
	 **/
	default String randomObjectID() {
		// Gets list of possible ID's
		Set<String> idSet = keySet();
		
		// Randomly pick and ID, and fetch the object
		int size = idSet.size();
		if (size > 0) {
			int chosen = ThreadLocalRandom.current().nextInt(size);
			int idx = 0;
			for (String idString : idSet) {
				if (idx >= chosen) {
					return idString;
				}
				idx++;
			}
		}
		
		// Possibly a blank set here, return null
		return null;
	}
	
	/**
	 * Gets and returns a random object,
	 * Useful for random validation / checks
	 *
	 * @return  Random DataObject
	 **/
	default FileWorkspace randomObject() {
		String oid = randomObjectID();
		if (oid != null) {
			return get(oid);
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
	 * Another way to phrase it, in worse case scenerio, its completely random,
	 * eventually iterating all objects. In best case scenerio, it does proper
	 * iteration as per normal.
	 *
	 * @param   Current object ID, can be NULL
	 *
	 * @return  Next object ID, if found
	 **/
	default String looselyIterateObjectID(String currentID) {
		// By default this is an inefficent implementation
		// of sorting the keyset, and returning in the respective order
		ArrayList<String> idList = new ArrayList<String>(keySet());
		Collections.sort(idList);
		int size = idList.size();
		
		// Blank set, nothing to iterate
		if (size == 0) {
			return null;
		}
		
		// But it works
		if (currentID == null) {
			// return first object
			return idList.get(0);
		}
		
		// Time to iterate it all
		for (int idx = 0; idx < size; ++idx) {
			if (currentID.equals(idList.get(idx))) {
				// Current position found
				if (idx >= (size - 1)) {
					// This is last object, return null (end)
					return null;
				}
				// Else get the next object
				return idList.get(idx + 1);
			}
			// If position not found, continue iterating
		}
		
		// If this position is reached,
		// possibly object was deleted mid iteration
		//
		// Fallsback to a random object to iterate
		return randomObjectID();
	}
	
	/**
	 * DataObject varient of randomlyIterateObjectID
	 *
	 * @param   DataObject to iterate next from, can be null
	 *
	 * @return  Random DataObject
	 **/
	default FileWorkspace looselyIterateObject(FileWorkspace currentObj) {
		String currentID = (currentObj != null) ? currentObj._oid() : null;
		String retID = looselyIterateObjectID(currentID);
		return (retID != null) ? get(retID) : null;
	}
}
