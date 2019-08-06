package picoded.dstack;

import picoded.core.conv.ArrayConv;
import picoded.core.conv.StringConv;

import java.io.File;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Set;

/**
 * Represent a file storage backend for a workspace
 * 
 * One of the major distinction between FileWorkspace, and "actual" file storage, is the intentional avoidence of most filesystem commands,
 * specifically folder commands. As this is modeled to have intercompatibility with object based storage systems instead of true file systems (with folder, and permissions)
 * 
 * As many of the S3-like object storage system has no concept of "directory" or "folders",
 * folder like features are emulated instead, to the minimum required in most use cases.
 * 
 * Another common functionality of these S3-like storage, is that if a file written / created for any path.
 * The preceding "folders" are considered to be automatically generated.
 */
public interface FileWorkspace {
	
	// FileWorkspace _oid, and timetstamp handling
	//--------------------------------------------------------------------------
	
	/**
	 * @return The object ID String
	 **/
	String _oid();
	
	/**
	 * The created timestamp of the map in ms,
	 * note that -1 means the current backend does not support this feature
	 *
	 * @return  DataObject created timestamp in ms
	 */
	default long createdTimestamp() {
		return -1;
	}
	
	/**
	 * The updated timestamp of the map in ms,
	 * note that -1 means the current backend does not support this feature
	 *
	 * @return  DataObject created timestamp in ms
	 */
	default long updatedTimestamp() {
		return -1;
	}
	
	/**
	 * Setup the current fileWorkspace within the fileWorkspaceMap,
	 *
	 * This ensures the workspace _oid is registered within the map,
	 * even if there is 0 files.
	 *
	 * Does not throw any error if workspace was previously setup
	 */
	void setupWorkspace();
	
	// File exists / removal
	//--------------------------------------------------------------------------
	
	/**
	 * Checks if the filepath exists with a file.
	 *
	 * @param  filepath in the workspace to check
	 *
	 * @return true, if file exists (and writable), false if it does not. (returns false if directory of the same name exists)
	 */
	boolean fileExist(final String filepath);
	
	/**
	 * Delete an existing file from the workspace
	 *
	 * @param filepath in the workspace to delete
	 */
	void removeFile(final String filepath);
	
	// Read / write byteArray information
	//--------------------------------------------------------------------------
	
	/**
	 * Reads the contents of a file into a byte array.
	 *
	 * @param  filepath in the workspace to extract
	 *
	 * @return the file contents, null if file does not exists
	 */
	byte[] readByteArray(final String filepath);
	
	/**
	 * Writes a byte array to a file creating the file if it does not exist.
	 *
	 * the parent directories of the file will be created if they do not exist.
	 *
	 * @param filepath in the workspace to extract
	 * @param data the content to write to the file
	 **/
	void writeByteArray(final String filepath, final byte[] data);
	
	/**
	 * Appends a byte array to a file creating the file if it does not exist.
	 *
	 * NOTE that by default this DOES NOT perform any file locks. As such,
	 * if used in a concurrent access situation. Segmentys may get out of sync.
	 *
	 * @param file   the file to write to
	 * @param data   the content to write to the file
	 **/
	default void appendByteArray(final String filepath, final byte[] data) {
		
		// Get existing data
		byte[] read = readByteArray(filepath);
		if (read == null) {
			writeByteArray(filepath, data);
		}
		
		// Append new data to existing data
		byte[] jointData = ArrayConv.addAll(read, data);
		
		// Write the new joint data
		writeByteArray(filepath, jointData);
	}
	
	//
	// String support for FileWorkspace
	//--------------------------------------------------------------------------
	
	default String readString(final String filepath) {
		return readString(filepath, "UTF-8");
	}
	
	default String readString(final String filepath, final String encoding) {
		byte[] result = readByteArray(filepath);
		return StringConv.fromByteArray(result, encoding);
		
	}
	
	default void writeString(final String filepath, String content) {
		writeString(filepath, content, "UTF-8");
	}
	
	default void writeString(final String filepath, String content, String encoding) {
		writeByteArray(filepath, StringConv.toByteArray(content, encoding));
	}
	
	//
	// Streaming support for FileWorkspace
	//--------------------------------------------------------------------------
	
	/**
	 * Get the input stream representation of a given filepath
	 * 
	 * @param filePath in the workspace to extract
	 * @return the file contents, null if file does not exists
	 */
	default InputStream readInputStream(final String filePath) {
		byte[] byteArr = readByteArray(filePath);
		return new ByteArrayInputStream(byteArr);
	}
	
	//
	// Folder Pathing support
	//--------------------------------------------------------------------------
	
	/**
	 * Delete an existing path from the workspace.
	 * This recursively removes all file content under the given path prefix
	 *
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 */
	void removeFolderPath(final String folderPath);
	
	/**
	 * Validate the given folder path exists.
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @return true if folderPath is valid
	 */
	boolean folderPathExist(final String folderPath);
	
	/**
	 * Automatically generate a given folder path if it does not exist
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 */
	void ensureFolderPath(final String folderPath);
	
	//
	// Move support
	//--------------------------------------------------------------------------
	
	/**
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
	 * @param sourceFile
	 * @param destinationFile
	 * 
	 */
	void moveFile(final String sourceFile, final String destinationFile);
	
	/**
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
	 * @param sourceFolder
	 * @param destinationFolder
	 * 
	 */
	void moveFolderPath(final String sourceFolder, final String destinationFolder);
	
	//
	// Listing support
	//--------------------------------------------------------------------------
	
	/**
	 * List all the various files and folders found in the given folderPath
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @param minDepth minimum depth count, before outputing the listing (uses a <= match)
	 * @param maxDepth maximum depth count, to stop the listing (-1 for infinite, uses a >= match)
	 * @return list of path strings - relative to the given folderPath (folders end with "/")
	 */
	Set<String> getFileAndFolderPathSet(final String folderPath, final int minDepth,
		final int maxDepth);
	
	/**
	 * List all the various files found in the given folderPath
	 * - min depth = 1
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @param maxDepth maximum depth count, to stop the listing
	 * @return list of path strings - relative to the given folderPath (folders end with "/")
	 */
	default Set<String> getFileAndFolderPathSet(final String folderPath, final int maxDepth) {
		return getFileAndFolderPathSet(folderPath, 1, maxDepth);
	}
	
	/**
	 * List all the various files found in the given folderPath
	 * - min depth = 1
	 * - max depth = 1
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @return list of path strings - relative to the given folderPath (folders end with "/")
	 */
	default Set<String> getFileAndFolderPathSet(final String folderPath) {
		return getFileAndFolderPathSet(folderPath, 1, 1);
	}
	
	/**
	 * List all the various files found in the given folderPath
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @param minDepth minimum depth count, before outputing the listing (uses a <= match)
	 * @param maxDepth maximum depth count, to stop the listing (-1 for infinite, uses a >= match)
	 * @return list of path strings - relative to the given folderPath
	 */
	Set<String> getFilePathSet(final String folderPath, final int minDepth, final int maxDepth);
	
	/**
	 * List all the various files found in the given folderPath
	 * - min depth = 1
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @param maxDepth maximum depth count, to stop the listing
	 * @return list of path strings - relative to the given folderPath
	 */
	default Set<String> getFilePathSet(final String folderPath, final int maxDepth) {
		return getFilePathSet(folderPath, 1, maxDepth);
	}
	
	/**
	 * List all the various files found in the given folderPath
	 * - min depth = 1
	 * - max depth = 1
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @return list of path strings - relative to the given folderPath
	 */
	default Set<String> getFilePathSet(final String folderPath) {
		return getFilePathSet(folderPath, 1, 1);
	}
	
	/**
	 * List all the various files found in the given folderPath
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @param minDepth minimum depth count, before outputing the listing (uses a <= match)
	 * @param maxDepth maximum depth count, to stop the listing
	 * @return list of path strings - relative to the given folderPath (folders end with "/")
	 */
	Set<String> getFolderPathSet(final String folderPath, final int minDepth, final int maxDepth);
	
	/**
	 * List all the various files found in the given folderPath
	 * - min depth = 1
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @param maxDepth maximum depth count, to stop the listing
	 * @return list of path strings - relative to the given folderPath (folders end with "/")
	 */
	default Set<String> getFolderPathSet(final String folderPath, final int maxDepth) {
		return getFolderPathSet(folderPath, 1, maxDepth);
	}
	
	/**
	 * List all the various files found in the given folderPath
	 * - min depth = 1
	 * - max depth = 1
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @return list of path strings - relative to the given folderPath (folders end with "/")
	 */
	default Set<String> getFolderPathSet(final String folderPath) {
		return getFolderPathSet(folderPath, 1, 1);
	}
	
}
