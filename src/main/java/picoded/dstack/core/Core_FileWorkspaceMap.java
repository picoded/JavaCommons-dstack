package picoded.dstack.core;

// Java imports

// Picoded imports
import picoded.dstack.*;

import java.util.List;

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
	public void setupWorkspace(String oid, String folderPath) {
		backend_setupWorkspace(oid, folderPath);
	}
	
	//--------------------------------------------------------------------------
	//
	// Functions, used by FileWorkspaceMap (to get / valdiate workspaces)
	// [Internal use, to be extended in future implementation]
	//
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
	 * The actual implementation to be completed in the subsequent classes that extends from Core_FileWorkspaceMap.
	 * List the files and folder recursively depending on the folderPath that was passed in.
	 *
	 * @param oid        of the workspace to search
	 * @param folderPath start of the folderPath to retrieve from
	 * @return back a list of Objects (the subsequent implementations will determine what Object is returned)
	 */
	abstract public FileNode backend_listWorkspaceTreeView(String oid, String folderPath, int depth);
	
	/**
	 * The actual implementation to be completed in the subsequent classes that extends from Core_FileWorkspaceMap.
	 * List the files and folder recursively depending on the folderPath that was passed in.
	 *
	 * @param oid        of the workspace to search
	 * @param folderPath start of the folderPath to retrieve from
	 * @param depth      the level of recursion that this is going to go to, -1 will be listing all the way
	 * @return back a list of Objects in list view
	 */
	abstract public List<FileNode> backend_listWorkspaceListView(String oid, String folderPath,
		int depth);
	
	abstract public boolean backend_moveFileInWorkspace(String oid, String source, String destination);
	
	//--------------------------------------------------------------------------
	//
	// Functions, used by FileWorkspace
	// [Internal use, to be extended in future implementation]
	//
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
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Removes the specified file path from the workspace in the backend
	 *
	 * @param oid identifier to the workspace
	 * @param filepath the file to be removed
	 */
	abstract public void backend_removeFile(final String oid, final String filepath);
	
	/**
	 * Setup the current fileWorkspace within the fileWorkspaceMap,
	 *
	 * This ensures the workspace _oid is registered within the map,
	 * even if there is 0 files.
	 *
	 * Does not throw any error if workspace was previously setup
	 */
	abstract public void backend_setupWorkspace(String oid, String folderPath);
	
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
