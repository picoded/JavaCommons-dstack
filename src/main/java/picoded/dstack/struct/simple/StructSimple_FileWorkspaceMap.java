package picoded.dstack.struct.simple;

import picoded.dstack.FileWorkspace;
import picoded.dstack.core.Core_FileWorkspaceMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StructSimple_FileWorkspaceMap extends Core_FileWorkspaceMap {
	
	protected ConcurrentHashMap<String, ConcurrentHashMap<String, byte[]>> fileContentMap = new ConcurrentHashMap<String, ConcurrentHashMap<String, byte[]>>();
	
	protected ReentrantReadWriteLock accessLock = new ReentrantReadWriteLock();
	
	//--------------------------------------------------------------------------
	//
	// Functions, used by FileWorkspace
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
			
			ConcurrentHashMap<String, byte[]> workspace = (fileContentMap.get(oid) == null) ? new ConcurrentHashMap<>()
				: fileContentMap.get(oid);
			
			workspace.put(filepath, data);
			
			fileContentMap.put(oid, workspace);
			
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
	
	/**
	 * Setup the current fileWorkspace within the fileWorkspaceMap,
	 *
	 * This ensures the workspace _oid is registered within the map,
	 * even if there is 0 files.
	 *
	 * Does not throw any error if workspace was previously setup
	 */
	@Override
	public void backend_setupWorkspace(String oid, String folderPath) {
		// do nothing for struct simple
	}
	
	//--------------------------------------------------------------------------
	//
	// WHAT IS THIS ???
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Removes the specified file path from the workspace in the backend
	 *
	 * @param oid identifier to the workspace
	 * @param filepath the file to be removed
	 */
	@Override
	public void backend_removePath(String oid, String filepath) {
		throw new RuntimeException("Not yet implemented");
	}
	
	@Override
	public boolean backend_moveFileInWorkspace(String oid, String source, String destination) {
		// do nothing for struct simple
		throw new RuntimeException("Not yet implemented");
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
