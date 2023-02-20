package picoded.dstack.mongodb;

// Java imports
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Date;

// JavaCommons imports
import picoded.core.common.EmptyArray;
import picoded.core.file.FileUtil;
import picoded.dstack.FileWorkspace;
import picoded.dstack.core.Core_FileWorkspaceMap;

import org.apache.commons.io.IOUtils;
// MongoDB imports
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.bson.conversions.Bson;
import com.mongodb.client.*;
import com.mongodb.client.gridfs.*;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

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
public class MongoDB_FileWorkspaceMap extends Core_FileWorkspaceMap {
	
	// --------------------------------------------------------------------------
	//
	// Constructor
	//
	// --------------------------------------------------------------------------
	
	/** MongoDB instance representing gridFS */
	GridFSBucket gridFSBucket = null;
	
	/** MongoDB instance representing the files and chunks collection (internal to the gridFSBucket) */
	MongoCollection<Document> filesCollection = null;
	MongoCollection<Document> chunksCollection = null;
	
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
			.withChunkSizeBytes(8 * 1000 * 1000);
		
		//
		// Note that we intentionally chose 8*1000*1000 chunk sizes
		// As this will give about 1-4kb space for chunk headers to
		// help ensure overall efficent chunk storage usage.
		//
		// This is due to the underlying storage rounding up to power
		// of 2 : https://jira.mongodb.org/browse/SERVER-13331
		//
		// Meaning a full "8 * 1000 * 1000" chunk would use "8 * 1024 * 1024"
		// worth of space, after adding the unknown headers (<=4kb of space : 8*24*24)
		//
		
		filesCollection = inStack.db_conn.getCollection(name + ".files");
		chunksCollection = inStack.db_conn.getCollection(name + ".chunks");
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
		
		// We insert a "root" object, to ensure the tables are initialized
		// ---
		if (!fullRawPathExist("root")) {
			setupAnchorFile_withFullRawPath("", "root", "root");
		}
		
		// Lets setup the index for the metadata fields (which is not enabled by default)
		// ---
		
		// Lets create the index for the oid
		IndexOptions opt = new IndexOptions();
		opt = opt.name("metadata.oid");
		filesCollection.createIndex(Indexes.ascending("oid"), opt);
		
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
		// The folder root, will only contain the "oid"
		return prefixPathExist(oid, null);
	}
	
	/**
	 * Setup the current fileWorkspace within the fileWorkspaceMap,
	 *
	 * This ensures the workspace oid is registered within the map,
	 * even if there is 0 files.
	 *
	 * Does not throw any error if workspace was previously setup
	 */
	@Override
	public void backend_setupWorkspace(String oid) {
		// We setup a blank file with type root
		if (!fullRawPathExist(oid)) {
			setupAnchorFile_withFullRawPath(oid, oid, "space");
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
		removeFilePathRecursively(oid, null);
	}
	
	//--------------------------------------------------------------------------
	//
	// Utility functions
	//
	//--------------------------------------------------------------------------
	
	/** Utility function used, to check if a workspace, or file exists **/
	protected boolean fullRawPathExist(String fullpath) {
		// Lets build the query for the "root file"
		Bson query = Filters.eq("filename", fullpath);
		
		// Lets prepare the search
		GridFSFindIterable search = gridFSBucket.find(query).limit(1);
		
		// Lets iterate the search result, and return true on an item
		try (MongoCursor<GridFSFile> cursor = search.iterator()) {
			if (cursor.hasNext()) {
				// ret.add(cursor.next().getString("oid"));
				return true;
			}
		}
		
		// Fail, as the search found no iterations
		return false;
	}
	
	/** Utility function used, to check if a folder, or file with folder prefix exists **/
	protected boolean prefixPathExist(String oid, String path) {
		// Lets build the query for the "root file"
		Bson query = null;
		
		// Handle search with null
		if (path == null || path.equals("")) {
			query = Filters.eq("metadata.oid", oid);
		} else {
			// Get the full prefixpath
			String fullPrefixPath = oid + "/" + path;
			
			// Remove matching path
			query = Filters.or(
				Filters.eq("filename", fullPrefixPath),
				Filters.and(Filters.eq("metadata.oid", oid),
					Filters.regex("filename", "^" + Pattern.quote(fullPrefixPath) + ".*")));
		}
		
		// Lets prepare the search
		GridFSFindIterable search = gridFSBucket.find(query).limit(1);
		
		// Lets iterate the search result, and return true on an item
		try (MongoCursor<GridFSFile> cursor = search.iterator()) {
			if (cursor.hasNext()) {
				return true;
			}
		}
		
		// No match found, fail
		return false;
	}
	
	/**
	 * Setup an empty file, used for various use cases
	 * The extended funciton name is intentional to avoid confusion of "full path" with "path"
	 */
	public void setupAnchorFile_withFullRawPath(String oid, String fullPath, String type) {
		// In general we will upload a blank file
		// with the relevent oid, that can be easily lookedup
		//
		// This is done using a closable input stream, with an empty byte array
		try (ByteArrayInputStream emptyStream = new ByteArrayInputStream(EmptyArray.BYTE)) {
			// Setup the metadata for the file
			Document metadata = new Document();
			metadata.append("oid", oid);
			metadata.append("type", type);
			
			// Prepare the upload options
			GridFSUploadOptions opt = (new GridFSUploadOptions()).metadata(metadata);
			ObjectId objID = gridFSBucket.uploadFromStream(fullPath, emptyStream, opt);
			
			// Flush it?
			objID.toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/** 
	 * Utility function used, to recursively delete all files within a specific path
	 **/
	protected void removeFilePathRecursively(String oid, String path) {
		// Lets build the query for the "root file"
		Bson query = null;
		
		if (path == null || path.equals("/") || path.isEmpty()) {
			// Remove everything under the oid
			query = Filters.eq("metadata.oid", oid);
		} else {
			// Remove matching path
			query = Filters.and(Filters.eq("metadata.oid", oid),
				Filters.regex("filename", "^" + Pattern.quote(oid + "/" + path) + ".*"));
		}
		
		// Lets prepare the search
		GridFSFindIterable search = gridFSBucket.find(query);
		
		// Lets iterate the search result, and return true on an item
		try (MongoCursor<GridFSFile> cursor = search.iterator()) {
			while (cursor.hasNext()) {
				GridFSFile fileObj = cursor.next();
				gridFSBucket.delete(fileObj.getId());
			}
		}
	}
	
	/** 
	 * Utility function used, to remove a specific file
	 **/
	protected boolean removeFilePath(String oid, String path) {
		// Lets build the query for the "root file"
		Bson query = null;
		
		// Remove matching path
		query = Filters.eq("filename", oid + "/" + path);
		
		// Lets prepare the search (removes all versions)
		GridFSFindIterable search = gridFSBucket.find(query);
		
		// Lets iterate the search result, and return true on an item
		boolean rmFlag = false;
		try (MongoCursor<GridFSFile> cursor = search.iterator()) {
			while (cursor.hasNext()) {
				GridFSFile fileObj = cursor.next();
				gridFSBucket.delete(fileObj.getId());
				rmFlag = true;
			}
		}
		
		// Return the remove status
		return rmFlag;
	}
	
	/** 
	 * Given the current path, enforce the parent pathing dir
	 * Used mainly to ensure "parent" folder exists on file write/rm
	 **/
	protected void ensureParentPath(String oid, String path) {
		// Ensure
		if (path == null || path.equals("/") || path.isEmpty()) {
			return;
		}
		
		// Cleanup ending slash
		if (path.endsWith("/")) {
			path = path.substring(0, path.length() - 1);
		}
		
		// Get the parent path
		String parPath = normalizeFolderPathString(FileUtil.getParentPath(path));
		
		// Does nothing if folder path is "blank"
		if (parPath == null || parPath.equals("/") || parPath.isEmpty()) {
			return;
		}
		
		// Path enforcement
		backend_ensureFolderPath(oid, parPath);
	}
	
	/**
	 * Because mongoDB does file versioining on each save, we would need to cleanup 
	 * older file versions where applicable, in a safe way
	 * 
	 * In general, due to the difficulty of possible race conditions that may occur
	 * when removing an "old version" immediately, that could be "read" mid-way.
	 * 
	 * First we scan for the list of all the file versions.
	 * 
	 * We find the latest that is at-least 10 seconds old (what we consider a safe window)
	 * and delete all version before it.
	 * 
	 * If after the above, we found that there are still "10 versions", as there were
	 * 10 writes in the past 10 seconds. We force a thread.sleep in increments of 1 second,
	 * and remove any versions that matches the above criteria. Up to a full 10 seconds of delay.
	 * 
	 * This will forcefully throttle down any write heavy flows, to avoid contentions.
	 * 
	 * This safety measure is used in addition, to the checks performed on file write
	 */
	protected void performVersionedFileCleanup(String oid, String path) {
		
		// Lets get the list of files and their respective versions
		// We query the file table directly, to reduce the required
		// back and forth queries
		
		// Get the full filename
		String filename = oid + "/" + path;
		
		// Get the current timestamp
		long now = System.currentTimeMillis();
		long tenSecondsAgo = now - (10 * 1000);
		
		// Lets fetch the full list in descending date order
		FindIterable<Document> search = filesCollection.find(Filters.eq("filename", filename));
		search = search.sort((new Document()).append("uploadDate", -1));
		
		// Lets remap from cursor to list
		List<Document> searchList = new ArrayList<>();
		try (MongoCursor<Document> cursor = search.iterator()) {
			while (cursor.hasNext()) {
				searchList.add(cursor.next());
			}
		}
		
		// Safe anchor point, all items after this is "safe to be deleted"
		// if this is detected properly (do not delete the safeAnchorPoint file itself)
		int safeAnchorPoint = -1;
		
		// Lets find the document thats atleast 10 seconds old
		for (int i = 1; i < searchList.size(); ++i) {
			Document doc = searchList.get(i);
			
			// Check if it meets the required timestamp
			if (doc.getDate("uploadDate").getTime() < tenSecondsAgo) {
				safeAnchorPoint = i;
				break;
			}
		}
		
		// Lets clear the old files, if safeAnchorPoint is found
		if (safeAnchorPoint >= 1) {
			// Lets loop through all items after the safeAnchorPoint
			while (searchList.size() > (safeAnchorPoint + 1)) {
				// Get and remove the last item
				Document doc = searchList.remove(searchList.size() - 1);
				ObjectId objID = doc.getObjectId("_id");
				
				// Lets remove the file (and its chunks)
				try {
					gridFSBucket.delete(objID);
				} catch (Exception e) {
					// do nothing, as there could be a race condition delete 
					// (2 delete by seperate write commands happenign together)
				}
			}
		}
		
		// If the list is less then 10, lets return
		if (searchList.size() <= 10) {
			return;
		}
		
		// We have more then 10 files, that is less then 10 seconds old
		// Lets do a forced 10 seconds halt, so we can forcefully clear the files
		try {
			Thread.sleep(10 * 1000);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		
		// And clear the various outdated files
		// after the latest, and its immediate previous version
		while (searchList.size() > 2) {
			// Get and remove the last item
			Document doc = searchList.remove(searchList.size() - 1);
			ObjectId objID = doc.getObjectId("_id");
			
			// Lets remove the file (and its chunks)
			try {
				gridFSBucket.delete(objID);
			} catch (Exception e) {
				// do nothing, as there could be a race condition delete 
				// (2 delete by seperate write commands happenign together)
			}
		}
		
	}
	
	//--------------------------------------------------------------------------
	//
	// File write
	// [Internal use, to be extended in future implementation]
	//
	//--------------------------------------------------------------------------
	
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
		
		// Build the full path
		String fullPath = oid + "/" + filepath;
		
		//
		// Due to the rather huge penalty of writing files, without actual content changes,
		// and the performance implications of a high number of back to back file changes.
		//
		// We will employ the following throttling safeguards
		//
		// 1) Throttling file writes, when the existing file is less then 2 seconds old
		// 2) Check against the current values, and skip the write if they match.
		//
		// This prevents the creation of a "new version" unless its needed. And slow down
		// any flooding of back to back file writes.
		//
		
		// 1) Lets check the previous write timing, and throttle it if needed
		// ---
		
		// Lets get the time "NOW"
		long now = System.currentTimeMillis();
		
		// Lets build the query for the file involved
		Bson query = Filters.eq("filename", fullPath);
		
		// Read timestamp, and objectid
		ObjectId readObjId = null;
		long readUploadTimestamp = -1;
		
		// Lets iterate the search result, and return true on an item
		try (MongoCursor<GridFSFile> cursor = gridFSBucket.find(query).limit(1).iterator()) {
			if (cursor.hasNext()) {
				GridFSFile fileObj = cursor.next();
				readUploadTimestamp = fileObj.getUploadDate().getTime();
				readObjId = fileObj.getObjectId();
			}
		}
		
		// Check if the current file is less then 2 seconds old
		// If so, we induce a wait for it to occur (if file exists)
		if (readObjId != null && readUploadTimestamp + 2000 >= now) {
			try {
				Thread.sleep(Math.min(Math.max(readUploadTimestamp + 2000 - now, 500), 2000));
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			
			// And get the latest objectID again (in case of any changes)
			try (MongoCursor<GridFSFile> cursor = gridFSBucket.find(query).limit(1).iterator()) {
				if (cursor.hasNext()) {
					GridFSFile fileObj = cursor.next();
					readUploadTimestamp = fileObj.getUploadDate().getTime();
					readObjId = fileObj.getObjectId();
				}
			}
		}
		
		// 2) Lets check against current value
		// ---
		
		// Handle null byte[]
		if (data == null) {
			data = EmptyArray.BYTE;
		}
		
		// Lets map the current value to an inputstream, in closable blocks
		// We intentionally use inputstream, to avoid needing 2 byte[] blocks in memory
		// (if file exists)
		if (readObjId == null) {
			// does nothing if the object does not exists
		} else {
			try (ByteArrayInputStream inBuffer = new ByteArrayInputStream(data)) {
				try (InputStream existingValue = gridFSBucket.openDownloadStream(readObjId)) {
					if (IOUtils.contentEquals(inBuffer, existingValue)) {
						// They are the same, skip the write
						return;
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		
		// Finally, lets write the update
		// ---
		
		try (ByteArrayInputStream inBuffer = new ByteArrayInputStream(data)) {
			// Setup the metadata for the file
			Document metadata = new Document();
			metadata.append("oid", oid);
			metadata.append("type", "file");
			
			// Prepare the upload options
			GridFSUploadOptions opt = (new GridFSUploadOptions()).metadata(metadata);
			ObjectId objID = gridFSBucket.uploadFromStream(fullPath, inBuffer, opt);
			objID.toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		// Perform post file write cleanup (if there was a previous version)
		if (readObjId != null) {
			performVersionedFileCleanup(oid, filepath);
		}
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Writes the full byte array of a file in the backend
	 * 
	 * This overwrite is useful for backends which supports this flow.
	 * Else it would simply be a wrapper over the non-stream version.
	 *
	 * @param   ObjectID of workspace
	 * @param   filepath to use for the workspace
	 * @param   data to write the file with
	 **/
	@Override
	public void backend_fileWriteInputStream(final String oid, final String filepath,
		InputStream data) {
		// Converts it to bytearray respectively
		byte[] rawBytes = null;
		try {
			rawBytes = IOUtils.toByteArray(data);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				data.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		// Does the bytearray writes
		backend_fileWrite(oid, filepath, rawBytes);
	}
	
	//--------------------------------------------------------------------------
	//
	// File read / exists
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
		// Get the buffer
		InputStream buffer = backend_fileReadInputStream(oid, filepath);
		
		// Null handling
		if (buffer == null) {
			return null;
		}
		
		// Cast the inputstream to byte[]
		byte[] ret = null;
		try {
			ret = IOUtils.toByteArray(buffer);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				buffer.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return ret;
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Get and return the stored data as a byte stream.
	 * 
	 * This overwrite is useful for backends which supports this flow.
	 * Else it would simply be a wrapper over the non-stream version.
	 *
	 * @param  ObjectID of workspace
	 * @param  filepath to use for the workspace
	 *
	 * @return  the stored byte array of the file
	 **/
	public InputStream backend_fileReadInputStream(String oid, String filepath) {
		try {
			return gridFSBucket.openDownloadStream(oid + "/" + filepath);
		} catch (Exception e) {
			if (e.getMessage().toLowerCase().indexOf("no file found") >= 0) {
				// Does nothing if no file is found
				return null;
			}
			
			// rethrow the error
			throw e;
		}
	}
	
	@Override
	public boolean backend_fileExist(String oid, String filepath) {
		// Check against the full file path
		return fullRawPathExist(oid + "/" + filepath);
	}
	
	@Override
	public void backend_removeFile(String oid, String filepath) {
		// Ensure root anchor exists
		backend_setupWorkspace(oid);
		// Ensure any parend dir anchor exists if needed
		ensureParentPath(oid, filepath);
		// Remove the respective file
		removeFilePath(oid, filepath);
	}
	
	// Folder Pathing support
	//--------------------------------------------------------------------------
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
		
		// Get the list of valid sub paths in the sourceFolder
		Set<String> subPath = backend_getFileAndFolderPathSet(oid, sourceFolder, -1, -1);
		
		//Special handling for the edge case where you try to move an empty folder and it just get removed instead
		if(subPath.isEmpty() && sourceFolder != null && sourceFolder !="" && sourceFolder != "/" && sourceFolder.length() > 0){
			subPath.add("/");
		}


		// Lets sync up all the folders first
		for (String dir : subPath) {
			if (dir.endsWith("/")) {
				backend_ensureFolderPath(oid, destinationFolder + dir);
			}
		}
		// Lets sync up all the files next
		for (String file : subPath) {
			if (file.endsWith("/") == false) {
				backend_copyFile(oid, sourceFolder + file, destinationFolder + file);
			}
		}
	}
	
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
		// Ensure root anchor exists
		backend_setupWorkspace(oid);
		// Ensure any parend dir anchor exists if needed
		ensureParentPath(oid, folderPath);

		// Remove the respective file
		removeFilePathRecursively(oid, folderPath);
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
		// Note that this passes if any of the files were created directly without folders
		return prefixPathExist(oid, folderPath);
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
		// We setup a blank file with type root, this checks only for the anchor file
		// if it does not exists, we will make it
		if (fullRawPathExist(oid + "/" + folderPath) == false) {
			setupAnchorFile_withFullRawPath(oid, oid + "/" + folderPath, "dir");
		}
	}
	
	//--------------------------------------------------------------------------
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
		
		// Currently only modified timestamp is supported
		return backend_modifiedTimestamp(oid, filepath);
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
		// Lets build the query for the "root file"
		Bson query = Filters.eq("filename", oid + "/" + filepath);
		
		// Lets prepare the search
		GridFSFindIterable search = gridFSBucket.find(query).limit(1);
		
		// Lets iterate the search result, and return true on an item
		try (MongoCursor<GridFSFile> cursor = search.iterator()) {
			if (cursor.hasNext()) {
				GridFSFile fileObj = cursor.next();
				return fileObj.getUploadDate().getTime();
			}
		}
		
		// Fail, as the search found no iterations
		return -1;
	}
	
	//--------------------------------------------------------------------------
	//
	// Query, and listing support
	//
	//--------------------------------------------------------------------------
	
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
	@Override
	public Set<String> backend_getFileAndFolderPathSet(final String oid, String folderPath,
		final int minDepth, final int maxDepth) {
		// Lets build the query for the "root file"
		Bson query = null;
		
		// The fuller prefix path
		String fullPrefixPath = oid + "/";
		
		if (folderPath == null || folderPath.equals("/") || folderPath.isEmpty()) {
			// Query everything (using only the oid)
			query = Filters.eq("metadata.oid", oid);
		} else {
			// Query using oid and the path
			fullPrefixPath = fullPrefixPath + folderPath;
			
			// Filter for matching path
			query = Filters.and(Filters.eq("metadata.oid", oid),
				Filters.regex("filename", "^" + Pattern.quote(fullPrefixPath) + ".*"));
		}
		
		// The return set
		Set<String> ret = new HashSet<>();
		
		// Lets prepare the search
		GridFSFindIterable search = gridFSBucket.find(query);
		
		// Lets iterate the search result, and return true on an item
		try (MongoCursor<GridFSFile> cursor = search.iterator()) {
			while (cursor.hasNext()) {
				// Get the fileobj and filename
				GridFSFile fileObj = cursor.next();
				String fullFilename = fileObj.getFilename();
				
				// Skip the oid anchor
				if (fullFilename.equals(oid)) {
					continue;
				}
				
				// Remove the oid prefix
				String filepath = fullFilename.substring(oid.length() + 1);
				
				// Register the validpath
				ret.add(filepath);
				
				// Prepare a clean path without ending slash
				String cleanPath = filepath;
				if (cleanPath.endsWith("/")) {
					cleanPath = cleanPath.substring(0, cleanPath.length() - 1);
				}
				
				// Lets split the filepath
				String[] cleanPathArr = cleanPath.split("/");
				List<String> cleanPathList = Arrays.asList(cleanPathArr);
				
				// Lets handle parent folders, note that i<cleanPathArr.length, 
				// already excludes the file itself
				for (int i = 1; i < cleanPathArr.length; ++i) {
					ret.add(String.join("/", cleanPathList.subList(0, i)) + "/");
				}
			}
		}
		
		// Filter and return the final set
		return backend_filterPathSet(ret, folderPath, minDepth, maxDepth, 0);
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
		DistinctIterable<String> search = filesCollection.distinct("metadata.oid", String.class);
		
		// Lets iterate the search
		try (MongoCursor<String> cursor = search.iterator()) {
			while (cursor.hasNext()) {
				String oid = cursor.next();
				// older version of DStack initialized a "root" folder, which is invalid
				if( oid == null || oid.equals("root") || oid.equals("") ) {
					continue;	
				}
				ret.add(oid);
			}
		}
		
		// Return the full keyset
		return ret;
	}
	
}
