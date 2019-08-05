package picoded.dstack.struct.simple;

import picoded.core.common.EmptyArray;
import picoded.core.file.FileUtil;
import picoded.dstack.FileWorkspace;
import picoded.dstack.core.Core_FileWorkspaceMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StructSimple_FileWorkspaceMap extends Core_FileWorkspaceMap {
	
	// Stores all the various data for structSimple
	protected ConcurrentHashMap<String, ConcurrentHashMap<String, byte[]>> fileContentMap = new ConcurrentHashMap<String, ConcurrentHashMap<String, byte[]>>();
	
	// Handles read / write locks
	protected ReentrantReadWriteLock accessLock = new ReentrantReadWriteLock();
	
	//--------------------------------------------------------------------------
	//
	// Internal functionality (StructSimple specific)
	//
	//--------------------------------------------------------------------------
	
	// internal blank byte[] used to represent a folder
	protected static byte[] FOLDER_OBJ = new byte[] {};
	
	/**
	 * Ensure the setup of folder, in the given workspace (initialized by OID)
	 * The calling function, MUST ensure that the appropriate write lock is performed.
	 * 
	 * @param  ObjectId of the workspace to get
	 * @param  folderPath to ensure (optional)
	 * 
	 * @return valid workspace map, with folderPath initialized
	 */
	protected ConcurrentHashMap<String, byte[]> noLock_setupWorkspaceFolderPath(final String oid,
		final String folderPath) {
		// Get the workspace map
		ConcurrentHashMap<String, byte[]> workspaceMap = fileContentMap.get(oid);
		
		// if workspace does not exist, set it up
		if (workspaceMap == null) {
			workspaceMap = new ConcurrentHashMap<>();
			fileContentMap.put(oid, workspaceMap);
		}
		
		// Null folder path = no setup
		if (folderPath == null) {
			return workspaceMap;
		}
		
		// Remove the starting and ending "/" in folderPath
		String reducedFolderPath = folderPath;
		if (reducedFolderPath.startsWith("/")) {
			reducedFolderPath = reducedFolderPath.substring(1);
		}
		if (reducedFolderPath.endsWith("/")) {
			reducedFolderPath = reducedFolderPath.substring(0, reducedFolderPath.length() - 1);
		}
		
		// Skip setup if blank
		if (reducedFolderPath.length() <= 0) {
			return workspaceMap;
		}
		
		// Alrighto, time to split up the folder path
		String[] splitFolderPath = reducedFolderPath.split("/");
		String dirPath = "";
		
		// and loop + initialize each one of them =x
		for (int i = 0; i < splitFolderPath.length; ++i) {
			// We store with ending "/"
			dirPath = dirPath + splitFolderPath[i] + "/";
			
			// And write a known blank byte[] (represents a folder)
			workspaceMap.put(dirPath, FOLDER_OBJ);
		}
		
		// Return the initialized workspaceMap
		return workspaceMap;
	}
	
	//--------------------------------------------------------------------------
	//
	// Workspace setup / exist funcitons
	// [Internal use, to be extended in future implementation]
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Checks and return of a workspace exists
	 *
	 * @param  Object ID of workspace to get
	 *
	 * @return  boolean to check if workspace exists
	 **/
	@Override
	public boolean backend_workspaceExist(String oid) {
		try {
			accessLock.readLock().lock();
			
			ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);
			return workspace != null;
		} finally {
			accessLock.readLock().unlock();
		}
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
	public void backend_setupWorkspace(String oid) {
		try {
			accessLock.writeLock().lock();
			
			// if workspace does not exist, set it up
			noLock_setupWorkspaceFolderPath(oid, "");
		} finally {
			accessLock.writeLock().unlock();
		}
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Removes the FileWorkspace, used to nuke an entire workspace
	 *
	 * @param ObjectID of workspace to remove
	 **/
	@Override
	public void backend_workspaceRemove(String oid) {
		try {
			accessLock.writeLock().lock();
			fileContentMap.remove(oid);
		} finally {
			accessLock.writeLock().unlock();
		}
	}
	
	//--------------------------------------------------------------------------
	//
	// File read / write
	// [Internal use, to be extended in future implementation]
	//
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
	@Override
	public byte[] backend_fileRead(String oid, String filepath) {
		try {
			accessLock.readLock().lock();
			
			ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);
			if (workspace != null && filepath != null) {
				return workspace.get(filepath);
			}
			return null;
		} finally {
			accessLock.readLock().unlock();
		}
		
	}
	
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
	public boolean backend_fileExist(final String oid, final String filepath) {
		try {
			accessLock.readLock().lock();
			
			ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);
			if (workspace != null && filepath != null) {
				return workspace.get(filepath) != null;
			}
		} finally {
			accessLock.readLock().unlock();
		}
		return false;
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Writes the full byte array of a file in the backend
	 *
	 * @param   ObjectID of workspace
	 * @param   filepath to use for the workspace
	 * @param   data to write the file with
	 **/
	@Override
	public void backend_fileWrite(String oid, String filepath, byte[] data) {
		try {
			accessLock.writeLock().lock();
			
			// Get workspace, with normalized parent path
			ConcurrentHashMap<String, byte[]> workspace = noLock_setupWorkspaceFolderPath(oid,
				FileUtil.getParentPath(filepath));
			
			// And put in the filepth data
			workspace.put(filepath, data);
			
		} finally {
			accessLock.writeLock().unlock();
		}
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Removes the specified file path from the workspace in the backend
	 *
	 * @param oid identifier to the workspace
	 * @param filepath the file to be removed
	 */
	@Override
	public void backend_removeFile(String oid, String filepath) {
		try {
			accessLock.writeLock().lock();
			
			ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);
			
			// workspace exist, remove the file in the workspace
			if (workspace != null) {
				workspace.remove(filepath);
			}
			
		} finally {
			accessLock.writeLock().unlock();
		}
	}
	
	//--------------------------------------------------------------------------
	//
	// Folder pathing support
	//
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
		try {
			accessLock.writeLock().lock();
			
			// Get the workspace, and abort if null
			ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);
			if (workspace == null) {
				return;
			}
			
			// Get the keyset - in a new hashset 
			// (so it wouldnt crash when we do modification)
			Set<String> allKeys = new HashSet<>(workspace.keySet());
			for (String key : allKeys) {
				// If folder path match - remove it
				if (key.startsWith(folderPath)) {
					workspace.remove(key);
				}
			}
			
		} finally {
			accessLock.writeLock().unlock();
		}
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
	public boolean backend_hasFolderPath(final String oid, final String folderPath) {
		try {
			accessLock.readLock().lock();
			ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);
			return workspace != null && workspace.get(folderPath) != null;
		} finally {
			accessLock.readLock().unlock();
		}
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
		try {
			accessLock.writeLock().lock();
			noLock_setupWorkspaceFolderPath(oid, folderPath);
		} finally {
			accessLock.writeLock().unlock();
		}
	}
	
	//--------------------------------------------------------------------------
	//
	// Constructor and maintenance
	//
	//--------------------------------------------------------------------------
	
	@Override
	public void systemSetup() {
		
	}
	
	@Override
	public void systemDestroy() {
		clear();
	}
	
	/**
	 * Maintenance step call, however due to the nature of most implementation not
	 * having any form of time "expiry", this call does nothing in most implementation.
	 *
	 * As such im making that the default =)
	 **/
	@Override
	public void maintenance() {
		// Do nothing
	}
	
	@Override
	public void clear() {
		try {
			accessLock.writeLock().lock();
			fileContentMap.clear();
		} finally {
			accessLock.writeLock().unlock();
		}
	}
}
