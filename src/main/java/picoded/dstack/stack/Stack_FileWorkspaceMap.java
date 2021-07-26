package picoded.dstack.stack;

import picoded.dstack.CommonStructure;
import picoded.dstack.core.Core_FileWorkspaceMap;

import java.util.List;
import java.util.Set;

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
			dataLayers[i].backend_moveFolderPath(oid, sourceFolder, destinationFolder);
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
