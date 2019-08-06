package picoded.dstack.core;

// Java imports
import java.io.File;
import java.util.*;

// Picoded imports
import picoded.core.conv.ArrayConv;
import picoded.core.file.FileUtil;
import picoded.dstack.DataObject;
import picoded.dstack.DataObjectMap;
import picoded.dstack.FileWorkspace;
import picoded.core.conv.ConvertJSON;
import picoded.core.conv.GUID;
import picoded.core.common.ObjectToken;

/**
 * Represents a single workspace in the FileWorkspaceMap collection.
 *
 * NOTE: This class should not be initialized directly, but through FileWorkspaceMap class
 **/
public class Core_FileWorkspace implements FileWorkspace {
	
	// Core variables
	//----------------------------------------------
	
	/**
	 * Core_FileWorkspaceMap used for the object
	 * Used to provide the underlying backend implementation
	 **/
	protected Core_FileWorkspaceMap main = null;
	
	/**
	 * GUID used for the object
	 **/
	protected String _oid = null;
	
	// Constructor
	//----------------------------------------------
	
	/**
	 * Setup a DataObject against a DataObjectMap backend.
	 *
	 * This allow the setup in the following modes
	 *
	 * + No (or invalid) GUID : Assume a new DataObject is made with NO DATA. Issues a new GUID for the object
	 * + GUID without remote data, will pull the required data when required
	 * + GUID with complete remote data
	 * + GUID with incomplete remote data, will pull the required data when required
	 *
	 * @param  Meta table to use
	 * @param  ObjectID to use, can be null
	 **/
	public Core_FileWorkspace(Core_FileWorkspaceMap inMain, String inOID) {
		// Main table to use
		main = (Core_FileWorkspaceMap) inMain;
		
		// Generates a GUID if not given
		if (inOID == null) {
			// Issue a GUID
			if (_oid == null) {
				_oid = GUID.base58();
			}
			
			if (_oid.length() < 4) {
				throw new RuntimeException("_oid should be atleast 4 character long");
			}
		} else {
			// _oid setup
			_oid = inOID;
		}
		
	}
	
	// FileWorkspace implementation
	//----------------------------------------------
	
	/**
	 * The object ID
	 **/
	@Override
	public String _oid() {
		return _oid;
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
	public void setupWorkspace() {
		main.setupWorkspace(_oid());
	}
	
	// File / Folder string normalization
	//--------------------------------------------------------------------------
	
	/**
	 * @param filePath
	 * @return filePath normalized to remove ending "/"
	 */
	private static String normalizeFilePathString(final String filePath) {
		if (filePath == null) {
			throw new IllegalArgumentException("Invalid null filePath");
		}
		
		String res = FileUtil.normalize(filePath, true);
		if (res.startsWith("/")) {
			res = res.substring(1);
		}
		if (res.endsWith("/")) {
			res = res.substring(0, res.length() - 1);
		}
		return res;
	}
	
	/**
	 * @param folderPath
	 * @return folderPath normalized with ending "/"
	 */
	private static String normalizeFolderPathString(final String folderPath) {
		if (folderPath == null || folderPath.length() <= 0) {
			return "/";
		}
		
		String res = FileUtil.normalize(folderPath, true);
		if (res.startsWith("/")) {
			res = res.substring(1);
		}
		if (!res.endsWith("/")) {
			res = res + "/";
		}
		return res;
	}
	
	// File exists checks
	//--------------------------------------------------------------------------
	
	/**
	 * Checks if the filepath exists with a file.
	 *
	 * @param  filepath in the workspace to check
	 *
	 * @return true, if file exists (and writable), false if it does not. Possible a folder
	 */
	public boolean fileExist(final String filepath) {
		return main.backend_fileExist(_oid, normalizeFilePathString(filepath));
	}
	
	/**
	 * Delete an existing file from the workspace
	 *
	 * @param filepath in the workspace to delete
	 */
	public void removeFile(final String filepath) {
		main.backend_removeFile(_oid, normalizeFilePathString(filepath));
	}
	
	// Read / write byteArray information
	//--------------------------------------------------------------------------
	
	/**
	 * Reads the contents of a file into a byte array.
	 *
	 * @param  filepath in the workspace to extract
	 *
	 * @return the file contents, null if file does not exists
	 */
	public byte[] readByteArray(final String filepath) {
		return main.backend_fileRead(_oid, normalizeFilePathString(filepath));
	}
	
	/**
	 * Writes a byte array to a file creating the file if it does not exist.
	 *
	 * the parent directories of the file will be created if they do not exist.
	 *
	 * @param filepath in the workspace to extract
	 * @param data the content to write to the file
	 **/
	public void writeByteArray(final String filepath, final byte[] data) {
		main.backend_fileWrite(_oid, normalizeFilePathString(filepath), data);
	}
	
	/**
	 * Appends a byte array to a file creating the file if it does not exist.
	 *
	 * NOTE that by default this DOES NOT perform any file locks. As such,
	 * if used in a concurrent access situation. Segmentys may get out of sync.
	 *
	 * @param file   the file to write to
	 * @param data   the content to write to the file
	 **/
	public void appendByteArray(final String filepath, final byte[] data) {
		// Normalize the file path
		String path = normalizeFilePathString(filepath);
		
		// Get existing data
		byte[] read = readByteArray(path);
		if (read == null) {
			writeByteArray(path, data);
		}
		
		// Append new data to existing data
		byte[] jointData = ArrayConv.addAll(read, data);
		
		// Write the new joint data
		writeByteArray(path, jointData);
	}
	
	// Folder Pathing support
	//--------------------------------------------------------------------------
	
	/**
	 * Delete an existing path from the workspace.
	 * This recursively removes all file content under the given path prefix
	 *
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 */
	public void removeFolderPath(final String folderPath) {
		main.backend_removeFolderPath(_oid, normalizeFolderPathString(folderPath));
	}
	
	/**
	 * Validate the given folder path exists.
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @return true if folderPath is valid
	 */
	public boolean hasFolderPath(final String folderPath) {
		return main.backend_hasFolderPath(_oid, normalizeFolderPathString(folderPath));
	}
	
	/**
	 * Automatically generate a given folder path if it does not exist
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 */
	public void ensureFolderPath(final String folderPath) {
		main.backend_ensureFolderPath(_oid, normalizeFolderPathString(folderPath));
	}
	
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
	 */
	public void moveFile(final String sourceFile, final String destinationFile) {
		main.backend_moveFile(_oid, normalizeFilePathString(sourceFile),
			normalizeFilePathString(destinationFile));
	}
	
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
	 */
	public void moveFolderPath(final String sourceFolder, final String destinationFolder) {
		main.backend_moveFolderPath(_oid, normalizeFolderPathString(sourceFolder),
			normalizeFolderPathString(destinationFolder));
	}
	
	// Listing support
	//--------------------------------------------------------------------------
	
	/**
	 * List all the various files found in the given folderPath
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @param minDepth minimum depth count, before outputing the listing (uses a <= match)
	 * @param maxDepth maximum depth count, to stop the listing (-1 for infinite, uses a >= match)
	 * @return list of path strings - relative to the given folderPath (folders end with "/")
	 */
	public Set<String> getFileAndFolderPathSet(final String folderPath, final int minDepth,
		final int maxDepth) {
		return main.backend_getFileAndFolderPathSet(_oid, normalizeFolderPathString(folderPath),
			minDepth, maxDepth);
	}
	
	/**
	 * List all the various files found in the given folderPath
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @param minDepth minimum depth count, before outputing the listing (uses a <= match)
	 * @param maxDepth maximum depth count, to stop the listing (-1 for infinite, uses a >= match)
	 * @return list of path strings - relative to the given folderPath
	 */
	public Set<String> getFilePathSet(final String folderPath, final int minDepth, final int maxDepth) {
		return main.backend_getFilePathSet(_oid, normalizeFolderPathString(folderPath), minDepth,
			maxDepth);
	}
	
	/**
	 * List all the various files found in the given folderPath
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @param minDepth minimum depth count, before outputing the listing (uses a <= match)
	 * @param maxDepth maximum depth count, to stop the listing
	 * @return list of path strings - relative to the given folderPath (folders end with "/")
	 */
	public Set<String> getFolderPathSet(final String folderPath, final int minDepth,
		final int maxDepth) {
		return main.backend_getFolderPathSet(_oid, normalizeFolderPathString(folderPath), minDepth,
			maxDepth);
	}
	
}
