package picoded.dstack.stack;

import picoded.dstack.CommonStructure;
import picoded.dstack.core.Core_FileWorkspaceMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;

/**
 * Stacked implementation of KeyValueMap data structure.
 *
 * Built ontop of the Core_KeyLongMap implementation.
 **/
public class Stack_FileWorkspaceMap extends Core_FileWorkspaceMap implements Stack_CommonStructure {
	
	//--------------------------------------------------------------------------
	//
	// Constructor vars
	//
	//--------------------------------------------------------------------------
	
	// Data layers to apply basic read/write against
	protected Core_FileWorkspaceMap[] dataLayers = null;
	
	// Data layer to apply query against
	protected Core_FileWorkspaceMap queryLayer = null;
	
	/**
	 * Setup the data object with the respective data, and query layers
	 *
	 * @param  inDataLayers data layers to get / set data from, 0 index first
	 * @param  inQueryLayer query layer for queries. Defaults to last data layer
	 */
	public Stack_FileWorkspaceMap(Core_FileWorkspaceMap[] inDataLayers,
		Core_FileWorkspaceMap inQueryLayer) {
		// Ensure that stack is configured with the respective datalayers
		if (inDataLayers == null || inDataLayers.length <= 0) {
			throw new IllegalArgumentException("Missing valid dataLayers configuration");
		}
		// Configure the query layer, to the last data layer if not set
		if (inQueryLayer == null) {
			inQueryLayer = inDataLayers[inDataLayers.length - 1];
		}
		dataLayers = inDataLayers;
		queryLayer = inQueryLayer;
	}
	
	/**
	 * Setup the data object with the respective data, and query layers
	 *
	 * @param  inDataLayers data layers to get / set data from, 0 index first;
	 *         query layer for queries. Defaults to last data layer
	 */
	public Stack_FileWorkspaceMap(Core_FileWorkspaceMap[] inDataLayers) {
		this(inDataLayers, null);
	}
	
	//--------------------------------------------------------------------------
	//
	// Interface to ovewrite for `Stack_CommonStructure` implmentation
	//
	//--------------------------------------------------------------------------
	
	/**
	 * @return  array of the internal common structure stack used by the Stack_ implementation
	 */
	public CommonStructure[] commonStructureStack() {
		return (CommonStructure[]) dataLayers;
	}
	
	//--------------------------------------------------------------------------
	//
	// Functions, used by FileWorkspace
	// [Internal use, to be extended in future implementation]
	//
	//--------------------------------------------------------------------------
	
	// Workspace operations
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 *
	 * Removes the FileWorkspace, used to nuke an entire workspace
	 *
	 * @param ObjectID of workspace to remove
	 **/
	@Override
	public void backend_workspaceRemove(String oid) {
		// Remove layer by layer starting from the lowest layer
		for (int i = dataLayers.length - 1; i >= 0; --i) {
			dataLayers[i].backend_workspaceRemove(oid);
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
	public boolean backend_workspaceExist(String oid) {
		// Once a workspace is found in any layers
		for (int i = 0; i < dataLayers.length; i++) {
			if (dataLayers[i].backend_workspaceExist(oid)) {
				return true;
			}
		}
		// If all layers did not find the workspace
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
		for (int i = dataLayers.length - 1; i >= 0; --i) {
			dataLayers[i].backend_setupWorkspace(oid);
		}
	}
	
	// File read and write using byte array
	//--------------------------------------------------------------------------
	
	/**
	 * Get and return the stored data as a byte[]
	 *
	 * @param  ObjectID of workspace
	 * @param  filepath to use for the workspace
	 *
	 * @return  the stored byte array of the file
	 **/
	@Override
	public byte[] backend_fileRead(String oid, String filepath) {
		// Retrieve from higher level to the source of truth
		for (int i = 0; i < dataLayers.length; ++i) {
			// Retrieve the data of the file
			byte[] data = dataLayers[i].backend_fileRead(oid, filepath);
			
			// Write back to the upper levels if data is found
			// return the data
			if (data != null) {
				for (i = i - 1; i >= 0; --i) {
					dataLayers[i].backend_fileWrite(oid, filepath, data);
				}
				return data;
			}
		}
		// No data exist
		return null;
		
	}
	
	/**
	 * Writes the full byte array of a file in the backend
	 *
	 * @param   ObjectID of workspace
	 * @param   filepath to use for the workspace
	 * @param   data to write the file with
	 **/
	@Override
	public void backend_fileWrite(String oid, String filepath, byte[] data) {
		// Write the data starting from the lowest layer
		for (int i = dataLayers.length - 1; i >= 0; --i) {
			dataLayers[i].backend_fileWrite(oid, filepath, data);
		}
	}
	
	// File read and write using byte stream
	//--------------------------------------------------------------------------
	
	/**
	 * Get and return the stored data as a InputStream
	 *
	 * @param  ObjectID of workspace
	 * @param  filepath to use for the workspace
	 *
	 * @return  the stored byte array of the file
	 **/
	@Override
	public InputStream backend_fileReadInputStream(final String oid, final String filepath) {
		
		// Due to the behaviour of how the file data needs to be handled across multiple layers
		// we only use an optimized "readStream" call if the filesystem is a single stack layer
		if (dataLayers.length == 1) {
			return dataLayers[0].backend_fileReadInputStream(oid, filepath);
		}
		
		// Fallback behaviour, polyfill the byte[] implementation
		//------------------------------------------------------------
		byte[] rawBytes = backend_fileRead(oid, filepath);
		if (rawBytes == null) {
			return null;
		}
		return new ByteArrayInputStream(rawBytes);
	}
	
	/**
	 * Writes the full by of a file in the backend
	 *
	 * @param   ObjectID of workspace
	 * @param   filepath to use for the workspace
	 * @param   data to write the file with
	 **/
	@Override
	public void backend_fileWriteInputStream(final String oid, final String filepath,
		final InputStream data) {
		
		//
		// Due to the behaviour of how the file data needs to be handled across multiple layers
		// we only use an optimized "writeStream" call ONLY if the filesystem is a single stack layer
		//
		// Else we will revert to byte[] that can be applied multiple times across the stack
		//
		if (dataLayers.length == 1) {
			dataLayers[0].backend_fileWriteInputStream(oid, filepath, data);
			return;
		}
		
		// Fallback behaviour, polyfill the byte[] implementation
		//------------------------------------------------------------
		
		// forward the null, and let the error handling below settle it
		if (data == null) {
			backend_fileWrite(oid, filepath, null);
		}
		
		// Converts it to bytearray respectively
		byte[] rawBytes = null;
		try {
			rawBytes = IOUtils.toByteArray(data);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		// Does the bytearray writes
		backend_fileWrite(oid, filepath, rawBytes);
	}
	
	// File exist / removal
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
	public boolean backend_fileExist(final String oid, final String filepath) {
		
		// Write the data starting from the lowest layer
		for (int i = dataLayers.length - 1; i >= 0; --i) {
			if (dataLayers[i].backend_fileExist(oid, filepath)) {
				return true;
			}
		}
		
		return false;
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
		// Remove the file starting from the lowest layer
		for (int i = dataLayers.length - 1; i >= 0; --i) {
			dataLayers[i].backend_removeFile(oid, filepath);
		}
	}
	
	//--------------------------------------------------------------------------
	//
	// Folder Pathing support
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
		for (int i = dataLayers.length - 1; i >= 0; --i) {
			dataLayers[i].backend_removeFolderPath(oid, folderPath);
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
	public boolean backend_folderPathExist(final String oid, final String folderPath) {
		for (int i = dataLayers.length - 1; i >= 0; --i) {
			if (dataLayers[i].backend_folderPathExist(oid, folderPath)) {
				return true;
			}
		}
		return false;
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
		for (int i = dataLayers.length - 1; i >= 0; --i) {
			dataLayers[i].backend_ensureFolderPath(oid, folderPath);
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
	 * The created timestamp of the map in ms,
	 * note that -1 means the current backend does not support this feature
	 *
	 * @param  ObjectID of workspace
	 * @param  filepath in the workspace to check
	 *
	 * @return  DataObject created timestamp in ms
	 */
	public long backend_createdTimestamp(final String oid, final String filepath) {
		// Once a workspace is found in any layers
		for (int i = 0; i < dataLayers.length; i++) {
			long ts = dataLayers[i].backend_createdTimestamp(oid, filepath);
			if (ts > 0) {
				return ts;
			}
		}
		// If all layers did not find the workspace
		return -1;
	}
	
	/**
	 * The modified timestamp of the map in ms,
	 * note that -1 means the current backend does not support this feature
	 *
	 * @param  ObjectID of workspace
	 * @param  filepath in the workspace to check
	 *
	 * @return  DataObject created timestamp in ms
	 */
	public long backend_modifiedTimestamp(final String oid, final String filepath) {
		// Once a workspace is found in any layers
		for (int i = 0; i < dataLayers.length; i++) {
			long ts = dataLayers[i].backend_modifiedTimestamp(oid, filepath);
			if (ts > 0) {
				return ts;
			}
		}
		// If all layers did not find the workspace
		return -1;
	}
	
	//--------------------------------------------------------------------------
	//
	// Move support
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * 
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
	 * @param  ObjectID of workspace
	 * @param  sourceFile
	 * @param  destinationFile
	 */
	public void backend_moveFile(final String oid, final String sourceFile,
		final String destinationFile) {
		for (int i = dataLayers.length - 1; i >= 0; --i) {
			dataLayers[i].backend_moveFile(oid, sourceFile, destinationFile);
		}
	}
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * 
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
	 * @param  ObjectID of workspace
	 * @param  sourceFolder
	 * @param  destinationFolder
	 * 
	 */
	public void backend_moveFolderPath(final String oid, final String sourceFolder,
		final String destinationFolder) {
		for (int i = dataLayers.length - 1; i >= 0; --i) {
			dataLayers[i].backend_moveFolderPath(oid, sourceFolder, destinationFolder);
		}
	}
	
	//--------------------------------------------------------------------------
	//
	// Move support
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * 
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
	 * @param  ObjectID of workspace
	 * @param  sourceFile
	 * @param  destinationFile
	 */
	public void backend_copyFile(final String oid, final String sourceFile,
		final String destinationFile) {
		for (int i = dataLayers.length - 1; i >= 0; --i) {
			dataLayers[i].backend_copyFile(oid, sourceFile, destinationFile);
		}
	}
	
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
		for (int i = dataLayers.length - 1; i >= 0; --i) {
			dataLayers[i].backend_copyFolderPath(oid, sourceFolder, destinationFolder);
		}
	}
	
	//--------------------------------------------------------------------------
	//
	// Listing support
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
	public Set<String> backend_getFileAndFolderPathSet(final String oid, final String folderPath,
		final int minDepth, final int maxDepth) {
		return queryLayer.backend_getFileAndFolderPathSet(oid, folderPath, minDepth, maxDepth);
	}
	
	/**
	 * List all the various files found in the given folderPath
	 * 
	 * @param  ObjectID of workspace
	 * @param  folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @param  minDepth minimum depth count, before outputing the listing (uses a <= match)
	 * @param  maxDepth maximum depth count, to stop the listing (-1 for infinite, uses a >= match)
	 * 
	 * @return list of path strings - relative to the given folderPath
	 */
	public Set<String> backend_getFilePathSet(final String oid, final String folderPath,
		final int minDepth, final int maxDepth) {
		return queryLayer.backend_getFilePathSet(oid, folderPath, minDepth, maxDepth);
	}
	
	/**
	 * List all the various files found in the given folderPath
	 * 
	 * @param  ObjectID of workspace
	 * @param  folderPath in the workspace (note, folderPath is normalized to end with "/")
	 * @param  minDepth minimum depth count, before outputing the listing (uses a <= match)
	 * @param  maxDepth maximum depth count, to stop the listing
	 * 
	 * @return list of path strings - relative to the given folderPath
	 */
	public Set<String> backend_getFolderPathSet(final String oid, final String folderPath,
		final int minDepth, final int maxDepth) {
		return queryLayer.backend_getFolderPathSet(oid, folderPath, minDepth, maxDepth);
	}
	
	//--------------------------------------------------------------------------
	//
	// KeySet support
	//
	//--------------------------------------------------------------------------
	
	/**
	 * List all the oid supported in the current backend
	 */
	public Set<String> keySet() {
		return queryLayer.keySet();
	}
	
	/**
	 * Gets and return a random object ID
	 *
	 * @return  Random object ID
	 **/
	public String randomObjectID() {
		return queryLayer.randomObjectID();
	}
	
	/**
	 * Gets and return the next object ID key for iteration given the current ID,
	 * null gets the first object in iteration.
	 *
	 * It is important to note actual iteration sequence is implementation dependent.
	 * And does not gurantee that newly added objects, after the iteration started,
	 * will be part of the chain of results.
	 *
	 * Similarly if the currentID was removed midway during iteration, the return
	 * result is not properly defined, and can either be null, or the closest object matched
	 * or even a random object.
	 *
	 * It is however guranteed, if no changes / writes occurs. A complete iteration
	 * will iterate all existing objects.
	 *
	 * The larger intention of this function, is to allow a background thread to slowly
	 * iterate across all objects, eventually. With an acceptable margin of loss on,
	 * recently created/edited object. As these objects will eventually be iterated in
	 * repeated rounds on subsequent calls.
	 *
	 * Due to its roughly random nature in production (with concurrent objects generated)
	 * and its iterative nature as an eventuality. The phrase looselyIterate was chosen,
	 * to properly reflect its nature.
	 *
	 * Another way to phrase it, in worse case scenerio, its completely random,
	 * eventually iterating all objects. In best case scenerio, it does proper
	 * iteration as per normal.
	 *
	 * @param   Current object ID, can be NULL
	 *
	 * @return  Next object ID, if found
	 **/
	public String looselyIterateObjectID(String currentID) {
		return queryLayer.looselyIterateObjectID(currentID);
	}
	
	//--------------------------------------------------------------------------
	//
	// Copy pasta code, I wished could have worked in an interface
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Removes all data, without tearing down setup
	 *
	 * Sadly, due to a how Map interface prevents "default" implementation
	 * of clear from being valid, this seems to be a needed copy-pasta code
	 **/
	public void clear() {
		for (CommonStructure layer : commonStructureStack()) {
			layer.clear();
		}
	}
}
