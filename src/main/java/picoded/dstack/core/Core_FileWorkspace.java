package picoded.dstack.core;

// Java imports
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
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
	
	/**
	 * Boolean flag, if true, indicates that the current FileWorkspace is uninitialized
	 * if so, we will setup the workspace if needed.
	 */
	protected boolean _isUninitialized = false;
	
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
				_isUninitialized = true;
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
		_isUninitialized = false;
	}
	
	/**
	 * Calls setupWorkspace if _isUninitialized is true
	 */
	protected void setupUninitializedWorkspace() {
		if (_isUninitialized) {
			setupWorkspace();
		}
	}
	
	// File / Folder string normalization
	//--------------------------------------------------------------------------
	
	/**
	 * @param filePath
	 * @return filePath normalized to remove ending "/"
	 */
	protected static String normalizeFilePathString(final String filePath) {
		return Core_FileWorkspaceMap.normalizeFilePathString(filePath);
	}
	
	/**
	 * @param folderPath
	 * @return folderPath normalized with ending "/"
	 */
	protected static String normalizeFolderPathString(final String folderPath) {
		return Core_FileWorkspaceMap.normalizeFolderPathString(folderPath);
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
		if (_isUninitialized) {
			return false;
		}
		return main.backend_fileExist(_oid, normalizeFilePathString(filepath));
	}
	
	/**
	 * Delete an existing file from the workspace
	 *
	 * @param filepath in the workspace to delete
	 */
	public void removeFile(final String filepath) {
		if (_isUninitialized) {
			return;
		}
		main.backend_removeFile(_oid, normalizeFilePathString(filepath));
	}
	
	// Read/write input/output stream
	//--------------------------------------------------------------------------
	
	/**
	 * Reads the contents of a file into a byte array.
	 *
	 * @param  filepath in the workspace to extract
	 *
	 * @return the file contents, null if file does not exists
	 */
	public InputStream readInputStream(final String filepath) {
		if (_isUninitialized) {
			return null;
		}
		return main.backend_fileReadInputStream(_oid, normalizeFilePathString(filepath));
	}
	
	/**
	 * Writes an output array to a file creating the file if it does not exist.
	 *
	 * the parent directories of the file will be created if they do not exist.
	 *
	 * @param filepath in the workspace to extract
	 * @param data the content to write to the file
	 **/
	public void writeInputStream(final String filepath, final InputStream data) {
		setupUninitializedWorkspace();
		main.backend_fileWriteInputStream(_oid, normalizeFilePathString(filepath), data);
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
		if (_isUninitialized) {
			return null;
		}
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
		setupUninitializedWorkspace();
		main.backend_fileWrite(_oid, normalizeFilePathString(filepath), data);
	}
	
	/**
	 * Appends a byte array to a file creating the file if it does not exist.
	 *
	 * NOTE that by default this DOES NOT perform any file locks. As such,
	 * if used in a concurrent access situation. Segments may get out of sync.
	 *
	 * @param file   the file to write to
	 * @param data   the content to write to the file
	 **/
	public void appendByteArray(final String filepath, final byte[] data) {
		setupUninitializedWorkspace();
		main.backend_fileAppendByteArray(_oid, normalizeFilePathString(filepath), data);
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
		if (_isUninitialized) {
			return;
		}
		main.backend_removeFolderPath(_oid, normalizeFolderPathString(folderPath));
	}
	
	/**
	 * Validate the given folder path exists.
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @return true if folderPath is valid
	 */
	public boolean folderPathExist(final String folderPath) {
		if (_isUninitialized) {
			return false;
		}
		return main.backend_folderPathExist(_oid, normalizeFolderPathString(folderPath));
	}
	
	/**
	 * Automatically generate a given folder path if it does not exist
	 * 
	 * @param folderPath in the workspace (note, folderPath is normalized to end with "/")
	 */
	public void ensureFolderPath(final String folderPath) {
		setupUninitializedWorkspace();
		main.backend_ensureFolderPath(_oid, normalizeFolderPathString(folderPath));
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
	 * The created timestamp of the map in ms,
	 * note that -1 means the current backend does not support this feature
	 *
	 * @param  filepath in the workspace to check
	 *
	 * @return  DataObject created timestamp in ms
	 */
	public long createdTimestamp(final String filepath) {
		if (_isUninitialized) {
			return -1;
		}
		return main.backend_createdTimestamp(_oid, normalizeFilePathString(filepath));
	}
	
	/**
	 * The modified timestamp of the map in ms,
	 * note that -1 means the current backend does not support this feature
	 *
	 * @param  filepath in the workspace to check
	 *
	 * @return  DataObject created timestamp in ms
	 */
	public long modifiedTimestamp(final String filepath) {
		if (_isUninitialized) {
			return -1;
		}
		return main.backend_modifiedTimestamp(_oid, normalizeFilePathString(filepath));
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
		if (_isUninitialized) {
			return;
		}
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
		if (_isUninitialized) {
			return;
		}
		main.backend_moveFolderPath(_oid, normalizeFolderPathString(sourceFolder),
			normalizeFolderPathString(destinationFolder));
	}
	
	// Copy support
	//--------------------------------------------------------------------------
	
	/**
	 * Copy a given file within the system
	 * 
	 * WARNING: Copy operations are typically not "atomic" in nature, and can be unsafe where
	 *          missing files / corrupted data can occur when executed concurrently with other operations.
	 * 
	 * In general "S3-like" object storage will not safely support atomic move operations.
	 * Please use the `atomicCopySupported()` function to validate if such operations are supported.
	 * 
	 * This operation may in effect function as a rename
	 * If the destionation file exists, it will be overwritten
	 * 
	 * @param sourceFile
	 * @param destinationFile
	 */
	public void copyFile(final String sourceFile, final String destinationFile) {
		if (_isUninitialized) {
			return;
		}
		main.backend_copyFile(_oid, normalizeFilePathString(sourceFile),
			normalizeFilePathString(destinationFile));
	}
	
	/**
	 * Copy a given file within the system
	 * 
	 * WARNING: Copy operations are typically not "atomic" in nature, and can be unsafe where
	 *          missing files / corrupted data can occur when executed concurrently with other operations.
	 * 
	 * In general "S3-like" object storage will not safely support atomic copy operations.
	 * Please use the `atomicCopySupported()` function to validate if such operations are supported.
	 * 
	 * Note that both source, and destionation folder will be normalized to include the "/" path.
	 * This operation may in effect function as a rename
	 * If the destionation folder exists with content, the result will be merged. With the sourceFolder files, overwriting on conflicts.
	 * 
	 * @param sourceFolder
	 * @param destinationFolder
	 */
	public void copyFolderPath(final String sourceFolder, final String destinationFolder) {
		if (_isUninitialized) {
			return;
		}
		main.backend_copyFolderPath(_oid, normalizeFolderPathString(sourceFolder),
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
		if (_isUninitialized) {
			return new HashSet<>();
		}
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
		if (_isUninitialized) {
			return new HashSet<>();
		}
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
		if (_isUninitialized) {
			return new HashSet<>();
		}
		return main.backend_getFolderPathSet(_oid, normalizeFolderPathString(folderPath), minDepth,
			maxDepth);
	}
	
}
