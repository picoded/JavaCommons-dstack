package picoded.dstack.file.simple;

import picoded.dstack.core.Core_DataObjectMap;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Reference implementation of DataObjectMap data structure using File
 * Has been implemented on the top of Core_DataObjectMap
 */
public class File_DataObjectMap extends Core_DataObjectMap {

	protected Map<String, Map<String, File>> fileMap = new HashMap<>();

	protected ReentrantReadWriteLock accessLock = new ReentrantReadWriteLock();

	public File_DataObjectMap(){ }

	/**
	 * Remove a particular
	 * @param oid
	 */
	@Override
	public void DataObjectRemoteDataMap_remove(String oid) {
		try {
			accessLock.writeLock().lock();
			Map<String, File> deleteMap = fileMap.get(oid);

			if (deleteMap != null) {
				for(Map.Entry<String, File> map : deleteMap.entrySet()) {
					// Let JVM handle it in case if it is not deleted successfully
					map.getValue().deleteOnExit();

					map.getValue().delete();
				}
				// Delete the key from the HashMap
				fileMap.remove(oid);
			}
		}
		finally {
			accessLock.writeLock().unlock();
		}
	}

	/**
	 * Fetch the particular fileMap by its ID
	 * @param oid
	 * @return
	 */
	@Override
	public Map<String, Object> DataObjectRemoteDataMap_get(String oid) {
		Map<String, Object> result = new HashMap<String, Object>();
		try {
			accessLock.readLock().lock();
			Map<String, File> storedFileMap = fileMap.get(oid);

			if (storedFileMap != null) {
				for (Entry<String, File> entry : storedFileMap.entrySet()) {
					result.put(entry.getKey(), deepCopy(storedFileMap.get(entry.getKey())));
				}
			}
		}
		finally {
			accessLock.readLock().unlock();
		}

		if (result.size() == 0) {
			return null;
		}
		return result;
	}

	/**
	 * Update the fileMap by its ID
	 * Either partial if supported else entire map
	 * @param oid
	 * @param fullMap
	 * @param keys
	 */
	@Override
	public void DataObjectRemoteDataMap_update(String oid, Map<String, Object> fullMap, Set<String> keys) {
		try {
			accessLock.writeLock().lock();

			// Get keys to store, null = all
			if (keys == null) {
				keys = fullMap.keySet();
			}

			// Makes a new map if needed
			Map<String, File> storedValue = fileMap.get(oid);
			if (storedValue == null) {
				storedValue = new HashMap<>();
			}

			// Get and store the required values
			for (String key : keys) {
				Object val = fullMap.get(key);
				if (val == null) {
					// If the fullMap does not contain a particular key we will delete it
					accessLock.writeLock().unlock();
					DataObjectRemoteDataMap_remove(key);
					accessLock.writeLock().lock();
				} else {

					// File must be created from the parent caller
					storedValue.put(key, (File) val);
				}
			}
			// Ensure the value map is stored
			fileMap.put(oid, storedValue);
		} finally {
			accessLock.writeLock().unlock();
		}
	}

	/**
	 * Removes all data, without tearing down setup
	 **/
	@Override
	public void systemSetup() {
		// Does not required
	}

	/**
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	@Override
	public void systemDestroy() {
		clear();
	}

	/**
	 * Removes all data, without tearing down setup
	 **/
	@Override
	public void clear() {
		try {
			accessLock.writeLock().lock();
			fileMap.clear();
			//toDO delete all the file recursively
		} finally {
			accessLock.writeLock().unlock();
		}
	}
}