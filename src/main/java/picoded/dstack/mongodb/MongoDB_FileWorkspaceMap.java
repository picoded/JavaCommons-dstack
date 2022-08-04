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
import org.bson.conversions.Bson;
import com.mongodb.client.*;
import com.mongodb.client.gridfs.*;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.Filters;

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
		// The folder root, will only contain the "oid"
		return fullRawPathExist(oid);
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
		// We setup a blank file with type root
		if(!fullRawPathExist(oid)) {
			setupAnchorFile(oid, oid, "root");
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
	
	/**
	 * Given a filepath, ensure a clean filepath (without starting "/")
	 */
	protected static String cleanFilePath(final String filepath) {
		// Note that the FileUtil.normalize step is not needed, as 
		// this is already done in the Core_FileWorkspaceMap 
		// ---
		// String cleanFilePath = FileUtil.normalize(filepath);

		// Cleanup the file apth
		String cleanFilePath = filepath;
		while (cleanFilePath.startsWith("/")) {
			cleanFilePath = cleanFilePath.substring(1);
		}
		return cleanFilePath;
	}
	
	/** Utility function used, to check if a workspace, or file exists **/
	protected boolean fullRawPathExist(String fullpath) {
		// Lets build the query for the "root file"
		Bson query = Filters.eq("filename", fullpath);
		
		// Lets prepare the search
		GridFSFindIterable search = gridFSBucket.find(query).limit(1);
		
		// Lets iterate the search result, and return true on an item
		try (MongoCursor<GridFSFile> cursor = search.iterator()) {
			if (cursor.hasNext()) {
				// ret.add(cursor.next().getString("_oid"));
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
		
		// Cleanup the path
		path = cleanFilePath(path);
		
		// Remove matching path
		query = Filters.and(
			Filters.eq("metadata._oid", oid),
			Filters.regex("filename", "^"+Pattern.quote(oid+"/"+path)+".*")
		);

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
	 */
	public void setupAnchorFile(String oid, String fullPath, String type) {
		// In general we will upload a blank file
		// with the relevent _oid, that can be easily lookedup
		//
		// This is done using a closable input stream, with an empty byte array
		try (ByteArrayInputStream emptyStream = new ByteArrayInputStream(EmptyArray.BYTE)) {
			// Setup the metadata for the file
			Document metadata = new Document();
			metadata.append("_oid", oid);
			metadata.append("type", type);
			
			// Prepare the upload options
			GridFSUploadOptions opt = (new GridFSUploadOptions()).metadata(metadata);
			gridFSBucket.uploadFromStream(fullPath, emptyStream, opt);
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
		
		if( path == null ) {
			// Remove everything under the oid
			query = Filters.eq("metadata._oid", oid);
		} else {
			// Cleanup the path
			path = cleanFilePath(path);
			
			// Remove matching path
			query = Filters.and(
				Filters.eq("metadata._oid", oid),
				Filters.regex("filename", "^"+Pattern.quote(oid+"/"+path)+".*")
			);
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
		
		// Cleanup the path
		path = cleanFilePath(path);
		
		// Remove matching path
		query = Filters.eq("filename", oid+"/"+path);

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
		// Build the input stream
		ByteArrayInputStream buffer = null;
		
		// Only build if its not null
		if (data != null) {
			buffer = new ByteArrayInputStream(data);
		}
		
		// Then pump it
		backend_fileWriteInputStream(oid, filepath, buffer);
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
	public void backend_fileWriteInputStream(String oid, String filepath, InputStream data) {
		// Get the clean file path
		String cleanPath = cleanFilePath(filepath);
		
		// Build the full path
		String fullPath = oid + "/" + cleanPath;
		
		if (data == null) {
			data = new ByteArrayInputStream(EmptyArray.BYTE);
		}
		
		// Write the file
		try {
			// Setup the metadata for the file
			Document metadata = new Document();
			metadata.append("_oid", oid);
			metadata.append("type", "file");
			
			// Prepare the upload options
			GridFSUploadOptions opt = (new GridFSUploadOptions()).metadata(metadata);
			gridFSBucket.uploadFromStream(fullPath, data, opt);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				data.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
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
		InputStream buffer = backend_fileReadInputStream(oid, filepath);
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
		return gridFSBucket.openDownloadStream(oid + "/" + cleanFilePath(filepath));
	}
	
	@Override
	public boolean backend_fileExist(String oid, String filepath) {
		// Check against the full file path
		return fullRawPathExist(oid + "/" + cleanFilePath(filepath));
	}
	
	@Override
	public void backend_removeFile(String oid, String filepath) {
		removeFilePath(oid, cleanFilePath(filepath));
	}
	
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
		removeFilePathRecursively(oid, cleanFilePath(folderPath));
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
		return prefixPathExist(oid, cleanFilePath(folderPath));
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
		// Cleanup folderPath
		String path = cleanFilePath(folderPath);

		// We setup a blank file with type root, this checks only for the anchor file
		// if it does not exists, we will make it
		if(!fullRawPathExist(oid+"/"+path)) {
			setupAnchorFile(oid, path, "dir");
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
		Bson query = Filters.eq("filename", cleanFilePath(filepath));
		
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
		
		// The fulle prefix path
		String fullPrefixPath = oid+"/";
		
		// Lets build the query, for fetchign the relevent items
		if( folderPath == null || folderPath.equals("/") || folderPath.isEmpty() ) {
			// Handles query for all folder paths
			query = Filters.eq("metadata._oid", oid);
		} else {
			// Cleanup the path
			folderPath = cleanFilePath(folderPath);
			fullPrefixPath = fullPrefixPath+folderPath;
			
			// Remove matching path
			query = Filters.and(
				Filters.eq("metadata._oid", oid),
				Filters.regex("filename", "^"+Pattern.quote(folderPath)+".*")
			);
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

				// Remove the oid prefix
				String filepath = fullFilename.substring( fullPrefixPath.length() );

				// Register the validpath
				ret.add(filepath);

				// Lets split the filepath
				String[] filepathArr = filepath.split("/");
				List<String> filepathList = Arrays.asList(filepathArr);

				// Lets handle parent folders
				for(int i=1+Math.max(minDepth,0); i<(filepathArr.length-1); ++i) {
					ret.add( String.join("/", filepathList.subList(0,i)+"/" ) );
				}
			}
		}

		// Filter and return the final set
		return backend_filtterPathSet( ret, folderPath, minDepth, maxDepth, 0);
	}
	
}
