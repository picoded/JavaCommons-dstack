package picoded.dstack.core;

// Java imports

// Picoded imports
import picoded.dstack.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Common base utility class of FileWorkspaceMap
 *
 * Does not actually implement its required feature,
 * but helps provide a common base line for all the various implementation.
 **/
abstract public class Core_FileWorkspaceMap extends Core_DataStructure<String, FileWorkspace>
	implements FileWorkspaceMap {
	
	//--------------------------------------------------------------------------
	//
	// FileWorkspace removal
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Removes a FileWorkspace if it exists, from the DB
	 *
	 * @param  object GUID to fetch, OR the FileWorkspace itself
	 *
	 * @return NULL
	 **/
	public FileWorkspace remove(Object key) {
		if (key instanceof FileWorkspace) {
			// Removal via FileWorkspace itself
			backend_workspaceRemove(((FileWorkspace) key)._oid());
		} else {
			// Remove using the ID
			backend_workspaceRemove(key.toString());
		}
		return null;
	}
	
	/**
	 * Setup the current fileWorkspace within the fileWorkspaceMap,
	 *
	 * This ensures the workspace _oid is registered within the map,
	 * even if there is 0 files.
	 *
	 * Does not throw any error if workspace was previously setup
	 */
	@Override
	public void setupWorkspace(String oid) {
		backend_setupWorkspace(oid);
	}
	
	//--------------------------------------------------------------------------
	//
	// Functions, used by FileWorkspaceMap
	// [Internal use, to be extended in future implementation]
	//
	//--------------------------------------------------------------------------
	
	// to get / valdiate workspaces
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Removes the FileWorkspace, used to nuke an entire workspace
	 *
	 * @param ObjectID of workspace to remove
	 **/
	abstract public void backend_workspaceRemove(String oid);
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Checks and return of a workspace exists
	 *
	 * @param  Object ID of workspace to get
	 *
	 * @return  boolean to check if workspace exists
	 **/
	abstract public boolean backend_workspaceExist(String oid);
	
	/**
	 * Setup the current fileWorkspace within the fileWorkspaceMap,
	 *
	 * This ensures the workspace _oid is registered within the map,
	 * even if there is 0 files.
	 *
	 * Does not throw any error if workspace was previously setup
	 */
	abstract public void backend_setupWorkspace(String oid);
	
	//--------------------------------------------------------------------------
	//
	// Functions, used by FileWorkspace
	// Note: It is safe to assume for all backend_* operations
	// that their filepath has been normalized by Core_FileWorkspace
	//
	// [Internal use, to be extended in future implementation]
	//
	//--------------------------------------------------------------------------
	
	// File exists / removal
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Get and return if the file exists, due to the potentially
	 * large size nature of files stored in FileWorkspace.
	 *
	 * Its highly recommended to optimize this function,
	 * instead of leaving it as default
	 *
	 * @param  ObjectID of workspace
	 * @param  filepath to use for the workspace
	 *
	 * @return  boolean true, if file eixst
	 **/
	abstract public boolean backend_fileExist(final String oid, final String filepath);
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Removes the specified file path from the workspace in the backend
	 *
	 * @param oid identifier to the workspace
	 * @param filepath the file to be removed
	 */
	abstract public void backend_removeFile(final String oid, final String filepath);
	
	// File read and write
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Get and return the stored data as a byte[]
	 *
	 * @param  ObjectID of workspace
	 * @param  filepath to use for the workspace
	 *
	 * @return  the stored byte array of the file
	 **/
	abstract public byte[] backend_fileRead(final String oid, final String filepath);
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Writes the full byte array of a file in the backend
	 *
	 * @param   ObjectID of workspace
	 * @param   filepath to use for the workspace
	 * @param   data to write the file with
	 **/
	abstract public void backend_fileWrite(final String oid, final String filepath, final byte[] data);
	
	// Folder Pathing support
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Delete an existing path from the workspace.
	 * This recursively removes all file content under the given path prefix
	 *
	 * @param  ObjectID of workspace
	 * @param  folderPath in the workspace (note, folderPath is normalized to end with "/")
	 *
	 * @return  the stored byte array of the file
	 **/
	public void backend_removeFolderPath(final String oid, final String folderPath) {
		throw new RuntimeException("Missing backend implementation");
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Validate the given folder path exists.
	 *
	 * @param  ObjectID of workspace
	 * @param  folderPath in the workspace (note, folderPath is normalized to end with "/")
	 *
	 * @return  the stored byte array of the file
	 **/
	public boolean backend_folderPathExist(final String oid, final String folderPath) {
		throw new RuntimeException("Missing backend implementation");
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Automatically generate a given folder path if it does not exist
	 *
	 * @param  ObjectID of workspace
	 * @param  folderPath in the workspace (note, folderPath is normalized to end with "/")
	 *
	 * @return  the stored byte array of the file
	 **/
	public void backend_ensureFolderPath(final String oid, final String folderPath) {
		throw new RuntimeException("Missing backend implementation");
	}
	
	// Move support
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * 
	 * Move a given file within the system
	 * 
	 * WARNING: Move operations are typically not "atomic" in nature, and can be unsafe where
	 *          missing files / corrupted data can occur when executed concurrently with other operations.
	 * 
	 * In general "S3-like" object storage will not safely support atomic move operations.
	 * Please use the `atomicMoveSupported()` function to validate if such operations are supported.
	 * 
	 * This operation may in effect function as a rename
	 * If the destionation file exists, it will be overwritten
	 * 
	 * @param  ObjectID of workspace
	 * @param  sourceFile
	 * @param  destinationFile
	 */
	public void backend_moveFile(final String oid, final String sourceFile,
		final String destinationFile) {
		throw new RuntimeException("Missing backend implementation");
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * 
	 * Move a given file within the system
	 * 
	 * WARNING: Move operations are typically not "atomic" in nature, and can be unsafe where
	 *          missing files / corrupted data can occur when executed concurrently with other operations.
	 * 
	 * In general "S3-like" object storage will not safely support atomic move operations.
	 * Please use the `atomicMoveSupported()` function to validate if such operations are supported.
	 * 
	 * Note that both source, and destionation folder will be normalized to include the "/" path.
	 * This operation may in effect function as a rename
	 * If the destionation folder exists with content, the result will be merged. With the sourceFolder files, overwriting on conflicts.
	 * 
	 * @param  ObjectID of workspace
	 * @param  sourceFolder
	 * @param  destinationFolder
	 * 
	 */
	public void backend_moveFolderPath(final String oid, final String sourceFolder,
		final String destinationFolder) {
		throw new RuntimeException("Missing backend implementation");
	}
	
	// Copy support
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * 
	 * Copy a given file within the system
	 * 
	 * WARNING: Copy operations are typically not "atomic" in nature, and can be unsafe where
	 *          missing files / corrupted data can occur when executed concurrently with other operations.
	 * 
	 * In general "S3-like" object storage will not safely support atomic copy operations.
	 * Please use the `atomicCopySupported()` function to validate if such operations are supported.
	 * 
	 * This operation may in effect function as a rename
	 * If the destionation file exists, it will be overwritten
	 * 
	 * @param  ObjectID of workspace
	 * @param  sourceFile
	 * @param  destinationFile
	 */
	public void backend_copyFile(final String oid, final String sourceFile,
		final String destinationFile) {
		throw new RuntimeException("Missing backend implementation");
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * 
	 * Copy a given file within the system
	 * 
	 * WARNING: Copy operations are typically not "atomic" in nature, and can be unsafe where
	 *          missing files / corrupted data can occur when executed concurrently with other operations.
	 * 
	 * In general "S3-like" object storage will not safely support atomic Copy operations.
	 * Please use the `atomicCopySupported()` function to validate if such operations are supported.
	 * 
	 * Note that both source, and destionation folder will be normalized to include the "/" path.
	 * This operation may in effect function as a rename
	 * If the destionation folder exists with content, the result will be merged. With the sourceFolder files, overwriting on conflicts.
	 * 
	 * @param  ObjectID of workspace
	 * @param  sourceFolder
	 * @param  destinationFolder
	 * 
	 */
	public void backend_copyFolderPath(final String oid, final String sourceFolder,
		final String destinationFolder) {
		throw new RuntimeException("Missing backend implementation");
	}
	
	//
	// Create and updated timestamp support
	//
	// Note that this feature does not have "normalized" support across
	// backend implementation, and is provided "as-it-is" for applicable
	// backend implementations.
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]

	 * The created timestamp of the map in ms,
	 * note that -1 means the current backend does not support this feature
	 *
	 * @param  ObjectID of workspace
	 * @param  filepath in the workspace to check
	 *
	 * @return  DataObject created timestamp in ms
	 */
	public long backend_createdTimestamp(final String oid, final String filepath) {
		return -1;
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 
	 * The modified timestamp of the map in ms,
	 * note that -1 means the current backend does not support this feature
	 *
	 * @param  ObjectID of workspace
	 * @param  filepath in the workspace to check
	 *
	 * @return  DataObject created timestamp in ms
	 */
	public long backend_modifiedTimestamp(final String oid, final String filepath) {
		return -1;
	}
	
	// Listing support
	//--------------------------------------------------------------------------
	
	/**
	 * Internal utility function used to filter a path set, and remove items that does not match
	 * 
	 * - its folderPath prefix
	 * - min/max depth
	 * - any / file / folder
	 * 
	 * @param rawSet
	 * @param folderPath
	 * @param minDepth
	 * @param maxDepth
	 * @param pathType (0 = any, 1 = file, 2 = folder)
	 * @return
	 */
	protected Set<String> backend_filtterPathSet(final Set<String> rawSet, final String folderPath,
		final int minDepth, final int maxDepth, final int pathType) {
		
		// Normalize the folder path
		String searchPath = folderPath;
		if (searchPath.equals("/")) {
			searchPath = "";
		}
		int searchPathLen = searchPath.length();
		
		// Return set
		Set<String> ret = new HashSet<>();
		
		// Get the keyset, and iterate it
		for (String key : rawSet) {
			
			// If folder does not match - skip
			if (searchPathLen > 0 && !key.startsWith(searchPath)) {
				continue;
			}
			
			// If folder path match - store it - maybe?
			String subPath = key.substring(searchPathLen);
			
			// No filtering is needed, store and continue
			if (maxDepth <= 0 && minDepth <= 0) {
				// Does nothing
			} else {
				// Lets perform path filtering
				
				// Lets filter out the ending "/" 
				String filteredSubPath = subPath;
				if (filteredSubPath.endsWith("/")) {
					filteredSubPath = filteredSubPath.substring(0, filteredSubPath.length());
				}
				
				// Split and count
				String[] splitSubPath = filteredSubPath.split("/");
				int subPathLength = (filteredSubPath.length() <= 0) ? 0 : splitSubPath.length;
				
				// Check min depth - skip key if check failed
				if (subPathLength < minDepth) {
					continue;
				}
				
				// Check max depth - skip key if check failed
				if (subPathLength > maxDepth) {
					continue;
				}
			}
			
			// Alrighto - lets check file / folder type - and add it in
			if (pathType == 1) {
				if (subPath.endsWith("/")) {
					// Not a file - abort!
					continue;
				}
			}
			if (pathType == 2) {
				if (!subPath.endsWith("/")) {
					// Not a folder - abort!
					continue;
				}
			}
			
			// Finally - all checks passed : add the path
			ret.add(subPath);
		}
		
		// Return the filtered set
		return ret;
	}
	
	/**
	 * List all the various files and folders found in the given folderPath
	 * 
	 * @param  ObjectID of workspace
	 * @param  folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @param  minDepth minimum depth count, before outputing the listing (uses a <= match)
	 * @param  maxDepth maximum depth count, to stop the listing (-1 for infinite, uses a >= match)
	 * 
	 * @return list of path strings - relative to the given folderPath (folders end with "/")
	 */
	public Set<String> backend_getFileAndFolderPathSet(final String oid, final String folderPath,
		final int minDepth, final int maxDepth) {
		throw new RuntimeException("Missing backend implementation");
	}
	
	/**
	 * List all the various files found in the given folderPath
	 * 
	 * @param  ObjectID of workspace
	 * @param  folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @param  minDepth minimum depth count, before outputing the listing (uses a <= match)
	 * @param  maxDepth maximum depth count, to stop the listing (-1 for infinite, uses a >= match)
	 * 
	 * @return list of path strings - relative to the given folderPath
	 */
	public Set<String> backend_getFilePathSet(final String oid, final String folderPath,
		final int minDepth, final int maxDepth) {
		// Get the full set
		Set<String> fullSet = backend_getFileAndFolderPathSet(oid, folderPath, minDepth, maxDepth);
		Set<String> retSet = new HashSet<>();
		
		// Iterate and filter for files
		for (String item : fullSet) {
			if (!item.endsWith("/")) {
				retSet.add(item);
			}
		}
		
		// Return the relevent set
		return retSet;
	}
	
	/**
	 * List all the various files found in the given folderPath
	 * 
	 * @param  ObjectID of workspace
	 * @param  folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @param  minDepth minimum depth count, before outputing the listing (uses a <= match)
	 * @param  maxDepth maximum depth count, to stop the listing
	 * 
	 * @return list of path strings - relative to the given folderPath
	 */
	public Set<String> backend_getFolderPathSet(final String oid, final String folderPath,
		final int minDepth, final int maxDepth) {
		// Get the full set
		Set<String> fullSet = backend_getFileAndFolderPathSet(oid, folderPath, minDepth, maxDepth);
		Set<String> retSet = new HashSet<>();
		
		// Iterate and filter for files
		for (String item : fullSet) {
			if (item.endsWith("/")) {
				retSet.add(item);
			}
		}
		
		// Return the relevent set
		return retSet;
	}
	
	//--------------------------------------------------------------------------
	//
	// FileWorkspace operations
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Generates a new blank object, with a GUID
	 *
	 * @return the FileWorkspace
	 **/
	public FileWorkspace newEntry() {
		// Actual return
		return new Core_FileWorkspace(this, null);
	}
	
	/**
	 * Get a FileWorkspace, and returns it. Skips existance checks if required
	 *
	 * @param  object GUID to fetch
	 * @param  boolean used to indicate if an existance check is done for the request
	 *
	 * @return the FileWorkspace
	 **/
	public FileWorkspace get(String oid, boolean isUnchecked) {
		if (isUnchecked) {
			return new Core_FileWorkspace(this, oid);
		} else {
			return get(oid);
		}
	}
	
	/**
	 * Get a FileWorkspace, and returns it.
	 *
	 * Existance checks is performed for such requests
	 *
	 * @param  object GUID to fetch
	 *
	 * @return the FileWorkspace, null if not exists
	 **/
	public FileWorkspace get(Object oid) {
		// String oid
		String soid = (oid != null) ? oid.toString() : null;
		
		// Return null, if OID is null
		if (soid == null || soid.isEmpty()) {
			return null;
		}
		
		// Get if workspace exists
		if (backend_workspaceExist(soid)) {
			return new Core_FileWorkspace(this, soid);
		}
		
		// Return null if not exist
		return null;
	}
	
	//--------------------------------------------------------------------------
	//
	// Constructor and maintenance
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Maintenance step call, however due to the nature of most implementation not
	 * having any form of time "expiry", this call does nothing in most implementation.
	 *
	 * As such im making that the default =)
	 **/
	@Override
	public void maintenance() {
		// Does nothing
	}
}
