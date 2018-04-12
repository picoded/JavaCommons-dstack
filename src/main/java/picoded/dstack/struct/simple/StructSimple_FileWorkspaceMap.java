package picoded.dstack.struct.simple;

import picoded.dstack.FileWorkspace;
import picoded.dstack.core.Core_FileWorkspaceMap;

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
	 * Removes the FileWorkspace, used to nuke an entire workspace
	 *
	 * @param ObjectID of workspace to remove
	 **/
	@Override
	protected void backend_workspaceRemove(String oid) {
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
	 * Checks and return of a workspace exists
	 *
	 * @param  Object ID of workspace to get
	 *
	 * @return  boolean to check if workspace exists
	 **/
	@Override
	protected boolean backend_workspaceExist(String oid) {
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
	 * Get and return the stored data as a byte[]
	 *
	 * @param  ObjectID of workspace
	 * @param  filepath to use for the workspace
	 *
	 * @return  the stored byte array of the file
	 **/
	@Override
	protected byte[] backend_fileRead(String oid, String filepath) {
		try {
			accessLock.readLock().lock();

			ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);

			if(workspace != null && filepath != null){
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
	 * Writes the full byte array of a file in the backend
	 *
	 * @param   ObjectID of workspace
	 * @param   filepath to use for the workspace
	 * @param   data to write the file with
	 **/
	@Override
	protected void backend_fileWrite(String oid, String filepath, byte[] data) {
		try {
			accessLock.writeLock().lock();

			ConcurrentHashMap<String, byte[]> workspace = (fileContentMap.get(oid) == null) ? new ConcurrentHashMap<>() : fileContentMap.get(oid);

			workspace.put(filepath, data);

			fileContentMap.put(oid, workspace);

		} finally {
			accessLock.writeLock().unlock();
		}
	}

	/**
	 * The basic initialization method for newEntry() so as to create an existence object
	 * to be searched.
	 * E.g. In file systems, the latter inheritance will implement a mkdir method if not exist
	 * Or using in-memory data structure, the latter inheritance will implement perhaps a
	 * HashMap<> object.
	 */
	@Override
	protected void init(String oid){
		fileContentMap.put(oid, new ConcurrentHashMap<String, byte[]>());
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
	 * having any form of time "expirary", this call does nothing in most implementation.
	 *
	 * As such im making that the default =)
	 **/
	@Override
	public void maintenance() {
		// Do nothing
	}

	@Override
	public void clear() {
		try{
			accessLock.writeLock().lock();
			fileContentMap.clear();
		} finally {
			accessLock.writeLock().unlock();
		}
	}
}
