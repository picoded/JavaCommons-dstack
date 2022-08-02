package picoded.dstack.mongodb;

import java.io.ByteArrayInputStream;
// Java imports
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// JavaCommons imports
import picoded.core.common.EmptyArray;
import picoded.core.file.FileUtil;
import picoded.dstack.FileWorkspace;
import picoded.dstack.core.Core_FileWorkspaceMap;

// MongoDB imports
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.conversions.Bson;
import com.mongodb.client.*;
import com.mongodb.client.gridfs.*;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;

/**
 * ## Purpose
 * Support MongoDB implementation of FileWorkspaceMap
 *
 * Built ontop of the Core_FileWorkspaceMap implementation.
 * 
 * ## Dev Notes
 * Developers of this class would need to reference the following in MongoDB
 * 
 * - GridFS : https://www.mongodb.com/docs/drivers/java/sync/current/fundamentals/gridfs/
 * - API: https://mongodb.github.io/mongo-java-driver/4.7/apidocs/mongodb-driver-sync/com/mongodb/client/gridfs/GridFSBucket.html
 **/
public class MongoDB_FileWorkspaceMap /* extends Core_FileWorkspaceMap */ {
	
	// --------------------------------------------------------------------------
	//
	// Constructor
	//
	// --------------------------------------------------------------------------
	
	/** MongoDB instance representing the backend connection */
	GridFSBucket gridFSBucket = null;
	
	/**
	 * Constructor, with name constructor
	 * 
	 * @param  inStack   hazelcast stack to use
	 * @param  name      of data object map to use
	 */
	public MongoDB_FileWorkspaceMap(MongoDBStack inStack, String name) {
		super();

		// Initialize the gridfs bucket, 
		// with the relevent DB, name, and config
		gridFSBucket = GridFSBuckets.create(inStack.db_conn, name) //
			.withChunkSizeBytes( 8 * 1000 * 1000 );

		//
		// Note that we intentionally chose 8*1000*1000 chunk sizes
		// As this will give about 1-4kb space for chunk headers to
		// help ensure overall efficent chunk storage usage.
		//
		// This is due to the underlying storage rounding up to power
		// of 2 : https://jira.mongodb.org/browse/SERVER-13331
		//
		// Meaning a full "8 * 1000 * 1000" chunk would use "8 * 1024 * 1024"
		// worth of space, after adding the unknown headers (<=2kb)
		//
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
	}
	
	/**
	 * Teardown and delete the backend storage table, etc. If needed
	 **/
	public void systemDestroy() {
		gridFSBucket.drop();
	}
	
	/**
	 * Removes all data, without tearing down setup
	 **/
	@Override
	public void clear() {
		gridFSBucket.drop();
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
		// Lets build the query for the "root file"
		Bson query = Filters.eq("filename", oid);

		// Lets prepare the search
		FindIterable<GridFSFile> search = gridFSBucket.find(query).limit(1);

		// Lets iterate the search result, and return true on an item
		try (MongoCursor<Document> cursor = search.iterator()) {
			while (cursor.hasNext()) {
				// ret.add(cursor.next().getString("_oid"));
				return true;
			}
		}

		// Fail, as the search found no iterations
		return false;
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
		// In general we will upload a blank file
		// with the relevent _oid, that can be easily lookedup
		//
		// This is done using a closable input stream, with an empty byte array
		try ( ByteArrayInputStream emptyStream = new ByteArrayInputStream(EmptyArray.BYTE) ) {
			// Setup the metadata for the file
			Document metadata = new Document();
			metadata.append("_oid", oid);
			metadata.append("type", "root");

			// Prepare the upload options
			GridFSUploadOptions opt = (new GridFSUploadOptions()).metadata(metadata);
			gridFSBucket.uploadFromStream(oid, emptyStream, opt);
		}
	}
	
	// /**
	//  * [Internal use, to be extended in future implementation]
	//  *
	//  * Removes the FileWorkspace, used to nuke an entire workspace
	//  *
	//  * @param ObjectID of workspace to remove
	//  **/
	// @Override
	// public void backend_workspaceRemove(String oid) {
	// 	try {
	// 		accessLock.writeLock().lock();
	// 		fileContentMap.remove(oid);
	// 	} finally {
	// 		accessLock.writeLock().unlock();
	// 	}
	// }
	
	// //--------------------------------------------------------------------------
	// //
	// // File read / write
	// // [Internal use, to be extended in future implementation]
	// //
	// //--------------------------------------------------------------------------
	
	// /**
	//  * [Internal use, to be extended in future implementation]
	//  *
	//  * Get and return the stored data as a byte[]
	//  *
	//  * @param  ObjectID of workspace
	//  * @param  filepath to use for the workspace
	//  *
	//  * @return  the stored byte array of the file
	//  **/
	// @Override
	// public byte[] backend_fileRead(String oid, String filepath) {
	// 	try {
	// 		accessLock.readLock().lock();
			
	// 		ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);
	// 		if (workspace != null && filepath != null) {
	// 			return workspace.get(filepath);
	// 		}
	// 		return null;
	// 	} finally {
	// 		accessLock.readLock().unlock();
	// 	}
		
	// }
	
	// /**
	//  * [Internal use, to be extended in future implementation]
	//  *
	//  * Get and return if the file exists, due to the potentially
	//  * large size nature of files stored in FileWorkspace.
	//  *
	//  * Its highly recommended to optimize this function,
	//  * instead of leaving it as default
	//  *
	//  * @param  ObjectID of workspace
	//  * @param  filepath to use for the workspace
	//  *
	//  * @return  boolean true, if file eixst
	//  **/
	// public boolean backend_fileExist(final String oid, final String filepath) {
	// 	try {
	// 		accessLock.readLock().lock();
			
	// 		ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);
	// 		if (workspace != null && filepath != null) {
	// 			return workspace.get(filepath) != null;
	// 		}
	// 	} finally {
	// 		accessLock.readLock().unlock();
	// 	}
	// 	return false;
	// }
	
	// /**
	//  * [Internal use, to be extended in future implementation]
	//  *
	//  * Writes the full byte array of a file in the backend
	//  *
	//  * @param   ObjectID of workspace
	//  * @param   filepath to use for the workspace
	//  * @param   data to write the file with
	//  **/
	// @Override
	// public void backend_fileWrite(String oid, String filepath, byte[] data) {
	// 	try {
	// 		accessLock.writeLock().lock();
			
	// 		// Get workspace, with normalized parent path
	// 		ConcurrentHashMap<String, byte[]> workspace = noLock_setupWorkspaceFolderPath(oid,
	// 			FileUtil.getParentPath(filepath));
			
	// 		// And put in the filepth data
	// 		workspace.put(filepath, data);
			
	// 	} finally {
	// 		accessLock.writeLock().unlock();
	// 	}
	// }
	
	// /**
	//  * [Internal use, to be extended in future implementation]
	//  *
	//  * Removes the specified file path from the workspace in the backend
	//  *
	//  * @param oid identifier to the workspace
	//  * @param filepath the file to be removed
	//  */
	// @Override
	// public void backend_removeFile(String oid, String filepath) {
	// 	try {
	// 		accessLock.writeLock().lock();
			
	// 		ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);
			
	// 		// workspace exist, remove the file in the workspace
	// 		if (workspace != null) {
	// 			workspace.remove(filepath);
	// 		}
			
	// 	} finally {
	// 		accessLock.writeLock().unlock();
	// 	}
	// }
	
	// //--------------------------------------------------------------------------
	// //
	// // Folder pathing support
	// //
	// //--------------------------------------------------------------------------
	
	// /**
	//  * [Internal use, to be extended in future implementation]
	//  *
	//  * Delete an existing path from the workspace.
	//  * This recursively removes all file content under the given path prefix
	//  *
	//  * @param  ObjectID of workspace
	//  * @param  folderPath in the workspace (note, folderPath is normalized to end with "/")
	//  *
	//  * @return  the stored byte array of the file
	//  **/
	// public void backend_removeFolderPath(final String oid, final String folderPath) {
	// 	try {
	// 		accessLock.writeLock().lock();
			
	// 		// Get the workspace, and abort if null
	// 		ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);
	// 		if (workspace == null) {
	// 			return;
	// 		}
			
	// 		// Get the keyset - in a new hashset 
	// 		// (so it wouldnt crash when we do modification)
	// 		Set<String> allKeys = new HashSet<>(workspace.keySet());
	// 		for (String key : allKeys) {
	// 			// If folder path match - remove it
	// 			if (key.startsWith(folderPath)) {
	// 				workspace.remove(key);
	// 			}
	// 		}
			
	// 	} finally {
	// 		accessLock.writeLock().unlock();
	// 	}
	// }
	
	// /**
	//  * [Internal use, to be extended in future implementation]
	//  *
	//  * Validate the given folder path exists.
	//  *
	//  * @param  ObjectID of workspace
	//  * @param  folderPath in the workspace (note, folderPath is normalized to end with "/")
	//  *
	//  * @return  the stored byte array of the file
	//  **/
	// public boolean backend_folderPathExist(final String oid, final String folderPath) {
	// 	try {
	// 		accessLock.readLock().lock();
	// 		ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);
	// 		return workspace != null && workspace.get(folderPath) != null;
	// 	} finally {
	// 		accessLock.readLock().unlock();
	// 	}
	// }
	
	// /**
	//  * [Internal use, to be extended in future implementation]
	//  *
	//  * Automatically generate a given folder path if it does not exist
	//  *
	//  * @param  ObjectID of workspace
	//  * @param  folderPath in the workspace (note, folderPath is normalized to end with "/")
	//  *
	//  * @return  the stored byte array of the file
	//  **/
	// public void backend_ensureFolderPath(final String oid, final String folderPath) {
	// 	try {
	// 		accessLock.writeLock().lock();
	// 		noLock_setupWorkspaceFolderPath(oid, folderPath);
	// 	} finally {
	// 		accessLock.writeLock().unlock();
	// 	}
	// }
	
	// //--------------------------------------------------------------------------
	// //
	// // Move support
	// //
	// //--------------------------------------------------------------------------
	
	// /**
	//  * @return if the current configured implementation supports atomic move operations.
	//  */
	// public boolean atomicMoveSupported() {
	// 	// True due to StructSimple use of a globle write lock
	// 	return true;
	// }
	
	// /**
	//  * [Internal use, to be extended in future implementation]
	//  * 
	//  * Move a given file within the system
	//  * 
	//  * WARNING: Move operations are typically not "atomic" in nature, and can be unsafe where
	//  *          missing files / corrupted data can occur when executed concurrently with other operations.
	//  * 
	//  * In general "S3-like" object storage will not safely support atomic move operations.
	//  * Please use the `atomicMoveSupported()` function to validate if such operations are supported.
	//  * 
	//  * This operation may in effect function as a rename
	//  * If the destionation file exists, it will be overwritten
	//  * 
	//  * @param  ObjectID of workspace
	//  * @param  sourceFile
	//  * @param  destinationFile
	//  */
	// public void backend_moveFile(final String oid, final String sourceFile,
	// 	final String destinationFile) {
	// 	try {
	// 		accessLock.writeLock().lock();
			
	// 		// Get the workspace, and abort if null
	// 		ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);
	// 		if (workspace == null) {
	// 			throw new RuntimeException("FileWorkspace does not exist : " + oid);
	// 		}
			
	// 		// Check if sourceFolder exist
	// 		if (workspace.get(sourceFile) == null) {
	// 			throw new RuntimeException("sourceFile does not exist (oid=" + oid + ") : "
	// 				+ sourceFile);
	// 		}
			
	// 		// Initialize the destionation folder
	// 		noLock_setupWorkspaceFolderPath(oid, FileUtil.getParentPath(destinationFile));
			
	// 		// Copy the file
	// 		workspace.put(destinationFile, workspace.get(sourceFile));
			
	// 		// And remove the old copy
	// 		workspace.remove(sourceFile);
	// 	} finally {
	// 		accessLock.writeLock().unlock();
	// 	}
	// }
	
	// /**
	//  * [Internal use, to be extended in future implementation]
	//  * 
	//  * Move a given file within the system
	//  * 
	//  * WARNING: Move operations are typically not "atomic" in nature, and can be unsafe where
	//  *          missing files / corrupted data can occur when executed concurrently with other operations.
	//  * 
	//  * In general "S3-like" object storage will not safely support atomic move operations.
	//  * Please use the `atomicMoveSupported()` function to validate if such operations are supported.
	//  * 
	//  * Note that both source, and destionation folder will be normalized to include the "/" path.
	//  * This operation may in effect function as a rename
	//  * If the destionation folder exists with content, the result will be merged. With the sourceFolder files, overwriting on conflicts.
	//  * 
	//  * @param  ObjectID of workspace
	//  * @param  sourceFolder
	//  * @param  destinationFolder
	//  * 
	//  */
	// public void backend_moveFolderPath(final String oid, final String sourceFolder,
	// 	final String destinationFolder) {
	// 	try {
	// 		accessLock.writeLock().lock();
			
	// 		// Get the workspace, and abort if null
	// 		ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);
	// 		if (workspace == null) {
	// 			throw new RuntimeException("FileWorkspace does not exist : " + oid);
	// 		}
			
	// 		// Check if sourceFolder exist
	// 		if (workspace.get(sourceFolder) == null) {
	// 			throw new RuntimeException("sourceFolder does not exist (oid=" + oid + ") : "
	// 				+ sourceFolder);
	// 		}
			
	// 		// Get the keyset - in a new hashset 
	// 		// (so it wouldnt crash when we do modification)
	// 		Set<String> allKeys = new HashSet<>(workspace.keySet());
	// 		for (String key : allKeys) {
	// 			// If folder path match - migrate it
	// 			if (key.startsWith(sourceFolder)) {
	// 				// Copy it over
	// 				workspace.put(destinationFolder + key.substring(sourceFolder.length()),
	// 					workspace.get(key));
	// 				// Remove it
	// 				workspace.remove(key);
	// 			}
	// 		}
			
	// 	} finally {
	// 		accessLock.writeLock().unlock();
	// 	}
	// }
	
	// //--------------------------------------------------------------------------
	// //
	// // Copy support
	// //
	// //--------------------------------------------------------------------------
	
	// /**
	//  * @return if the current configured implementation supports atomic Copy operations.
	//  */
	// public boolean atomicCopySupported() {
	// 	// True due to StructSimple use of a globle write lock
	// 	return true;
	// }
	
	// /**
	//  * [Internal use, to be extended in future implementation]
	//  * 
	//  * Copy a given file within the system
	//  * 
	//  * WARNING: Copy operations are typically not "atomic" in nature, and can be unsafe where
	//  *          missing files / corrupted data can occur when executed concurrently with other operations.
	//  * 
	//  * In general "S3-like" object storage will not safely support atomic Copy operations.
	//  * Please use the `atomicCopySupported()` function to validate if such operations are supported.
	//  * 
	//  * This operation may in effect function as a rename
	//  * If the destionation file exists, it will be overwritten
	//  * 
	//  * @param  ObjectID of workspace
	//  * @param  sourceFile
	//  * @param  destinationFile
	//  */
	// public void backend_copyFile(final String oid, final String sourceFile,
	// 	final String destinationFile) {
	// 	try {
	// 		accessLock.writeLock().lock();
			
	// 		// Get the workspace, and abort if null
	// 		ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);
	// 		if (workspace == null) {
	// 			throw new RuntimeException("FileWorkspace does not exist : " + oid);
	// 		}
			
	// 		// Check if sourceFolder exist
	// 		if (workspace.get(sourceFile) == null) {
	// 			throw new RuntimeException("sourceFile does not exist (oid=" + oid + ") : "
	// 				+ sourceFile);
	// 		}
			
	// 		// Initialize the destionation folder
	// 		noLock_setupWorkspaceFolderPath(oid, FileUtil.getParentPath(destinationFile));
			
	// 		// Copy the file
	// 		workspace.put(destinationFile, workspace.get(sourceFile));
	// 	} finally {
	// 		accessLock.writeLock().unlock();
	// 	}
	// }
	
	// /**
	//  * [Internal use, to be extended in future implementation]
	//  * 
	//  * Copy a given file within the system
	//  * 
	//  * WARNING: Copy operations are typically not "atomic" in nature, and can be unsafe where
	//  *          missing files / corrupted data can occur when executed concurrently with other operations.
	//  * 
	//  * In general "S3-like" object storage will not safely support atomic Copy operations.
	//  * Please use the `atomicCopySupported()` function to validate if such operations are supported.
	//  * 
	//  * Note that both source, and destionation folder will be normalized to include the "/" path.
	//  * This operation may in effect function as a rename
	//  * If the destionation folder exists with content, the result will be merged. With the sourceFolder files, overwriting on conflicts.
	//  * 
	//  * @param  ObjectID of workspace
	//  * @param  sourceFolder
	//  * @param  destinationFolder
	//  * 
	//  */
	// public void backend_copyFolderPath(final String oid, final String sourceFolder,
	// 	final String destinationFolder) {
	// 	try {
	// 		accessLock.writeLock().lock();
			
	// 		// Get the workspace, and abort if null
	// 		ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);
	// 		if (workspace == null) {
	// 			throw new RuntimeException("FileWorkspace does not exist : " + oid);
	// 		}
			
	// 		// Check if sourceFolder exist
	// 		if (workspace.get(sourceFolder) == null) {
	// 			throw new RuntimeException("sourceFolder does not exist (oid=" + oid + ") : "
	// 				+ sourceFolder);
	// 		}
			
	// 		// Get the keyset - in a new hashset 
	// 		// (so it wouldnt crash when we do modification)
	// 		Set<String> allKeys = new HashSet<>(workspace.keySet());
	// 		for (String key : allKeys) {
	// 			// If folder path match - migrate it
	// 			if (key.startsWith(sourceFolder)) {
	// 				// Copy it over
	// 				workspace.put(destinationFolder + key.substring(sourceFolder.length()),
	// 					workspace.get(key));
	// 			}
	// 		}
			
	// 	} finally {
	// 		accessLock.writeLock().unlock();
	// 	}
	// }
	
	// //--------------------------------------------------------------------------
	// //
	// // Listing support
	// //
	// //--------------------------------------------------------------------------
	
	// /**
	//  * List all the various files and folders found in the given folderPath
	//  * 
	//  * @param  ObjectID of workspace
	//  * @param  folderPath in the workspace (note, folderPath is normalized to end with "/")
	//  * @param  minDepth minimum depth count, before outputing the listing (uses a <= match)
	//  * @param  maxDepth maximum depth count, to stop the listing (-1 for infinite, uses a >= match)
	//  * 
	//  * @return list of path strings - relative to the given folderPath (folders end with "/")
	//  */
	// public Set<String> backend_getFileAndFolderPathSet(final String oid, final String folderPath,
	// 	final int minDepth, final int maxDepth) {
	// 	try {
	// 		accessLock.readLock().lock();
			
	// 		// Get the workspace, and abort if null
	// 		ConcurrentHashMap<String, byte[]> workspace = fileContentMap.get(oid);
	// 		if (workspace == null) {
	// 			throw new RuntimeException("FileWorkspace does not exist : " + oid);
	// 		}
			
	// 		// Check if folderPath exist
	// 		String searchPath = folderPath;
	// 		if (searchPath.equals("/")) {
	// 			searchPath = "";
	// 		}
	// 		if (searchPath.length() > 0 && workspace.get(searchPath) == null) {
	// 			throw new RuntimeException("folderPath does not exist (oid=" + oid + ") : "
	// 				+ searchPath);
	// 		}
			
	// 		// Return a filtered set
	// 		return backend_filtterPathSet(workspace.keySet(), searchPath, minDepth, maxDepth, 0);
	// 	} finally {
	// 		accessLock.readLock().unlock();
	// 	}
	// }
	
	// //--------------------------------------------------------------------------
	// //
	// // Constructor and maintenance
	// //
	// //--------------------------------------------------------------------------
	
	// @Override
	// public void systemSetup() {
		
	// }
	
	// @Override
	// public void systemDestroy() {
	// 	clear();
	// }
	
	// /**
	//  * Maintenance step call, however due to the nature of most implementation not
	//  * having any form of time "expiry", this call does nothing in most implementation.
	//  *
	//  * As such im making that the default =)
	//  **/
	// @Override
	// public void maintenance() {
	// 	// Do nothing
	// }
	
	// @Override
	// public void clear() {
	// 	try {
	// 		accessLock.writeLock().lock();
	// 		fileContentMap.clear();
	// 	} finally {
	// 		accessLock.writeLock().unlock();
	// 	}
	// }
}