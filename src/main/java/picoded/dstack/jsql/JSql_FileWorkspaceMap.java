package picoded.dstack.jsql;

import picoded.dstack.core.Core_FileWorkspaceMap;
import picoded.core.conv.ListValueConv;
import picoded.core.file.FileUtil;
import picoded.core.struct.GenericConvertList;
import picoded.dstack.connector.jsql.JSql;
import picoded.dstack.connector.jsql.JSqlResult;

import java.util.HashSet;
import java.util.Set;

public class JSql_FileWorkspaceMap extends Core_FileWorkspaceMap {
	
	//--------------------------------------------------------------------------
	//
	// Constructor setup
	//
	//--------------------------------------------------------------------------
	
	/**
	 * The inner sql object
	 **/
	protected JSql sqlObj = null;
	
	/**
	 * The tablename for the key value pair map
	 **/
	protected String fileWorkspaceTableName = null;
	
	/**
	 * The tablename the parent key
	 **/
	protected String primaryKeyTable = null;
	
	public JSql_FileWorkspaceMap(JSql inJSql, String tablename) {
		super();
		sqlObj = inJSql;
		fileWorkspaceTableName = tablename;
	}
	
	//--------------------------------------------------------------------------
	//
	// Internal config vars
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Primary key type
	 **/
	protected String pKeyColumnType = "BIGINT PRIMARY KEY AUTOINCREMENT";
	
	/**
	 * Timestamp field type
	 **/
	protected String tStampColumnType = "BIGINT";
	
	/**
	 * Key name field type
	 **/
	protected String keyColumnType = "VARCHAR(64)";
	
	/**
	 * Path field type
	 **/
	protected String pathColumnType = "VARCHAR(255)";
	
	/**
	 * Interal flag used to indicate a file (1), or a folder (2)
	 */
	protected String fTypColumnType = "SMALLINT";
	
	/**
	 * Raw datastorage type
	 **/
	protected String rawDataColumnType = "BLOB";
	
	// fTyp flags
	private static int fTyp_file = 1;
	private static int fTyp_folder = 2;
	
	//--------------------------------------------------------------------------
	//
	// Constructor and maintenance
	//
	//--------------------------------------------------------------------------
	
	@Override
	public void systemSetup() {
		try {
			sqlObj.createTable(fileWorkspaceTableName, new String[] { //
				"pKy", // Primary key
					// Time stamps
					"cTm", //object created time
					"uTm", //object updated time
					"eTm", //object expire time (for future use)
					"fTyp", //file type
					// Object keys
					"oID", //_oid
					"path", // relative file path
					"data" // actual file content
				}, //
				new String[] { //
				pKeyColumnType, //Primary key
					// Time stamps
					tStampColumnType, //
					tStampColumnType, //
					tStampColumnType, //
					fTypColumnType, //
					// Object keys
					keyColumnType, //
					// Value storage
					pathColumnType, //
					rawDataColumnType } //
				);
			
			// Unique index
			//------------------------------------------------
			sqlObj.createIndex( //
				fileWorkspaceTableName, "oID, path", "UNIQUE", "unq" //
			);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		
	}
	
	@Override
	public void systemDestroy() {
		sqlObj.delete(fileWorkspaceTableName);
	}
	
	@Override
	public void clear() {
		sqlObj.delete(fileWorkspaceTableName);
	}
	
	//--------------------------------------------------------------------------
	//
	// Workspace Setup
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
		sqlObj.delete(fileWorkspaceTableName, "oID = ?", new Object[] { oid });
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
		JSqlResult jSqlResult = sqlObj.select(fileWorkspaceTableName, "oID", "oID = ?",
			new Object[] { oid }, null, 1, 0);
		return jSqlResult.rowCount() > 0;
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
		// Setup a blank folder path
		long now = JSql_DataObjectMapUtil.getCurrentTimestamp();
		sqlObj.upsert( //
			fileWorkspaceTableName, //
			new String[] { "oID", "path" }, //
			new Object[] { oid, "" }, //
			new String[] {}, //
			new Object[] {}, //
			new String[] { "uTm", "cTm", "fTyp", "eTm", "data" }, //
			new Object[] { now, now, fTyp_folder, 0, null }, //
			null // The only misc col, is pKy, which is being handled by DB
			);
	}
	
	//--------------------------------------------------------------------------
	//
	// File read and write
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
		JSqlResult jSqlResult = sqlObj.select(fileWorkspaceTableName, "data",
			"oID = ? AND path = ? AND fTyp = ?", new Object[] { oid, filepath, fTyp_file });
		if (jSqlResult == null || jSqlResult.get("data") == null || jSqlResult.rowCount() <= 0) {
			return null;
		}
		return (byte[]) jSqlResult.get("data").get(0);
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
		JSqlResult jSqlResult = sqlObj.select(fileWorkspaceTableName, "pKy",
			"oID = ? AND path = ? AND fTyp = ?", new Object[] { oid, filepath, fTyp_file });
		return jSqlResult.rowCount() > 0;
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
		// Setup parent folders
		backend_ensureFolderPath(oid, FileUtil.getParentPath(filepath));
		
		// Write the file
		long now = JSql_DataObjectMapUtil.getCurrentTimestamp();
		sqlObj.upsert( //
			fileWorkspaceTableName, //
			new String[] { "oID", "path" }, //
			new Object[] { oid, filepath }, //
			new String[] { "uTm", "data" }, //
			new Object[] { now, data }, //
			new String[] { "cTm", "eTm", "fTyp" }, //
			new Object[] { now, 0, fTyp_file }, //
			null // The only misc col, is pKy, which is being handled by DB
			);
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
		sqlObj.delete(fileWorkspaceTableName, "oid = ? AND path = ?", new Object[] { oid, filepath });
	}
	
	//--------------------------------------------------------------------------
	//
	// Folder path handling
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
		String formattedPath = folderPath.replaceAll("\\%", "\\%");
		sqlObj.delete(fileWorkspaceTableName, "oID = ? AND (path = ? OR path LIKE ?)", new Object[] {
			oid, folderPath, formattedPath + "%" });
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
		JSqlResult jSqlResult = sqlObj.select(fileWorkspaceTableName, "oID",
			"oID = ? AND path = ? AND fTyp = ?", new Object[] { oid, folderPath, fTyp_folder }, null,
			1, 0);
		return jSqlResult.rowCount() > 0;
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
		// Null folder path = no setup
		if (folderPath == null) {
			return;
		}
		
		// Remove the starting and ending "/" in folderPath
		String reducedFolderPath = folderPath;
		if (reducedFolderPath.startsWith("/")) {
			reducedFolderPath = reducedFolderPath.substring(1);
		}
		if (reducedFolderPath.endsWith("/")) {
			reducedFolderPath = reducedFolderPath.substring(0, reducedFolderPath.length() - 1);
		}
		
		// Skip setup if blank
		if (reducedFolderPath.length() <= 0) {
			return;
		}
		
		// Alrighto, time to split up the folder path
		String[] splitFolderPath = reducedFolderPath.split("/");
		String dirPath = "";
		long now = JSql_DataObjectMapUtil.getCurrentTimestamp();
		
		// and loop + initialize each one of them =x
		for (int i = 0; i < splitFolderPath.length; ++i) {
			// We store with ending "/"
			dirPath = dirPath + splitFolderPath[i] + "/";
			
			// And upsert the folder setup
			sqlObj.upsert( //
				fileWorkspaceTableName, //
				new String[] { "oID", "path" }, //
				new Object[] { oid, dirPath }, //
				new String[] {}, //
				new Object[] {}, //
				new String[] { "uTm", "cTm", "fTyp", "eTm", "data" }, //
				new Object[] { now, now, fTyp_folder, 0, null }, //
				null // The only misc col, is pKy, which is being handled by DB
				);
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
	public void backend_moveFile(final String oid, final String sourceFile,
		final String destinationFile) {
		
		// Abort if file does not exist
		if (!backend_fileExist(oid, sourceFile)) {
			throw new RuntimeException("sourceFile does not exist (oid=" + oid + ") : " + sourceFile);
		}
		
		// Setup parent folders
		backend_ensureFolderPath(oid, FileUtil.getParentPath(destinationFile));
		
		// Remove the old file (if exist)
		backend_removeFile(oid, destinationFile);
		
		// Apply the update statement
		sqlObj.prepareStatement(
			"UPDATE " + fileWorkspaceTableName + " SET path = ? WHERE oid = ? AND path = ?",
			destinationFile, oid, sourceFile).update();
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
		// First lets get all the various paths
		Set<String> affectedPaths = backend_getFileAndFolderPathSet(oid, sourceFolder, -1, -1);
		
		// Setup parent folders
		backend_ensureFolderPath(oid, FileUtil.getParentPath(destinationFolder));
		
		// For each path, lets do the respective delete + update
		for (String subPath : affectedPaths) {
			// Delete destination path / file (if exists)
			sqlObj.delete(fileWorkspaceTableName, "oid = ? AND path = ?", new Object[] { oid,
				destinationFolder + subPath });
			
			// Apply the update statement
			sqlObj.prepareStatement(
				"UPDATE " + fileWorkspaceTableName + " SET path = ? WHERE oid = ? AND path = ?",
				destinationFolder + subPath, oid, sourceFolder + subPath).update();
		}
		
		// Update the destination directory itself
		sqlObj.delete(fileWorkspaceTableName, "oid = ? AND path = ?", new Object[] { oid,
			destinationFolder });
		// Apply the update statement
		sqlObj.prepareStatement(
			"UPDATE " + fileWorkspaceTableName + " SET path = ? WHERE oid = ? AND path = ?",
			destinationFolder, oid, sourceFolder).update();
	}
	
	//--------------------------------------------------------------------------
	//
	// Copy support
	//
	//--------------------------------------------------------------------------
	
	/**
	 * [Internal use, to be extended in future implementation]
	 * 
	 * Copy a given file within the system
	 * 
	 * WARNING: Copy operations are typically not "atomic" in nature, and can be unsafe where
	 *          missing files / corrupted data can occur when executed concurrently with other operations.
	 * 
	 * In general "S3-like" object storage will not safely support atomic copy operations.
	 * Please use the `atomicCopySupported()` function to validate if such operations are supported.
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
		
		// Abort if file does not exist
		if (!backend_fileExist(oid, sourceFile)) {
			throw new RuntimeException("sourceFile does not exist (oid=" + oid + ") : " + sourceFile);
		}
		
		// Setup parent folders
		backend_ensureFolderPath(oid, FileUtil.getParentPath(destinationFile));
		
		// Read the file content
		byte[] data = backend_fileRead(oid, sourceFile);
		// Get current timestamp
		long now = JSql_DataObjectMapUtil.getCurrentTimestamp();
		// Upsert into database
		sqlObj.upsert( //
			fileWorkspaceTableName, //
			// Unique values to "INSERT" or "UPDATE" on
			new String[] { "oID", "path" }, //
			new Object[] { oid, destinationFile }, //
			// Values that require updating 
			new String[] { "uTm", "data" }, //
			new Object[] { now, data }, //
			// Values if exists, do NOT update them (aka ignored in UPDATE)
			new String[] { "cTm", "eTm", "fTyp" }, //
			new Object[] { now, 0, fTyp_file }, //
			// Additional collumns that exist in the database table
			// to provide special handling.
			null // The only misc col, is pKy, which is being handled by DB
			);
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
		// First lets get all the various paths
		Set<String> affectedPaths = backend_getFileAndFolderPathSet(oid, sourceFolder, -1, -1);
		
		// Setup destination folders
		backend_ensureFolderPath(oid, destinationFolder);
		
		// Get current timestamp
		long now = JSql_DataObjectMapUtil.getCurrentTimestamp();
		
		// For each path, lets scan for folders to recursively initialize
		for (String subPath : affectedPaths) {
			// Folders would end with the reserved "/" character
			if (subPath.endsWith("/")) {
				// Upsert statement for folder
				sqlObj.upsert( //
					fileWorkspaceTableName, //
					// Unique values to "INSERT" or "UPDATE" on
					new String[] { "oID", "path" }, //
					new Object[] { oid, destinationFolder + subPath }, //
					// Values that require updating 
					new String[] { "uTm", "data" }, //
					new Object[] { now, null }, //
					// Values if exists, do NOT update them (aka ignored in UPDATE)
					new String[] { "cTm", "eTm", "fTyp" }, //
					new Object[] { now, 0, fTyp_folder }, //
					// Additional collumns that exist in the database table
					// to provide special handling.
					null // The only misc col, is pKy, which is being handled by DB
					);
			}
		}
		
		// For each path, lets scan for files (not a folder)
		for (String subPath : affectedPaths) {
			// Setup the various sub directories
			if (!subPath.endsWith("/")) {
				// Read the file content
				byte[] data = backend_fileRead(oid, sourceFolder + subPath);
				// And copy over the file content
				sqlObj.upsert( //
					fileWorkspaceTableName, //
					// Unique values to "INSERT" or "UPDATE" on
					new String[] { "oID", "path" }, //
					new Object[] { oid, destinationFolder + subPath }, //
					// Values that require updating 
					new String[] { "uTm", "data" }, //
					new Object[] { now, data }, //
					// Values if exists, do NOT update them (aka ignored in UPDATE)
					new String[] { "cTm", "eTm", "fTyp" }, //
					new Object[] { now, 0, fTyp_file }, //
					// Additional collumns that exist in the database table
					// to provide special handling.
					null // The only misc col, is pKy, which is being handled by DB
					);
			}
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
		
		// The full JSQL result
		JSqlResult jSqlResult = null;
		
		// Search for all in a workspace
		if (folderPath.equals("/") || folderPath.length() <= 0) {
			jSqlResult = sqlObj
				.select(fileWorkspaceTableName, "path", "oID = ?", new Object[] { oid });
		} else {
			// Prepare and execute the selection query
			String formattedPath = folderPath.replaceAll("\\%", "\\%");
			jSqlResult = sqlObj.select(fileWorkspaceTableName, "path",
				"oID = ? AND (path = ? OR path LIKE ?)", new Object[] { oid, folderPath,
					formattedPath + "%" });
		}
		
		// Throw an error if no path was found
		if (jSqlResult == null || jSqlResult.get("path") == null || jSqlResult.rowCount() <= 0) {
			throw new RuntimeException("folderPath does not exist (oid=" + oid + ") : " + folderPath);
		}
		
		// Lets prepare a raw set
		GenericConvertList<Object> pathList = jSqlResult.get("path");
		Set<String> rawSet = new HashSet<>();
		int pathListLen = pathList.size();
		
		// For each item in the list, setup the set
		for (int i = 0; i < pathListLen; ++i) {
			rawSet.add(pathList.getString(i));
		}
		
		// Filter and return it accordingly
		return backend_filterPathSet(rawSet, folderPath, minDepth, maxDepth, 0);
	}
	
	/**
	 * Return the current list of ID keys for fileWorkspaceMap
	 **/
	@Override
	public Set<String> keySet() {
		// Search blank "paths" which represents the workspace root
		JSqlResult r = sqlObj.select(fileWorkspaceTableName, "oID", "path = ?", new Object[] { "" });
		
		// Convert it into a set
		if (r == null || r.get("oID") == null) {
			return new HashSet<String>();
		}
		return ListValueConv.toStringSet(r.getObjectList("oID"));
	}
	
	/**
	 * Gets and return a random object ID
	 *
	 * @return  Random object ID
	 **/
	public String randomObjectID() {
		// Get a random ID
		JSqlResult r = sqlObj.randomSelect(fileWorkspaceTableName, "oID", null, null, 1);
		
		// No result : NULL
		if (r == null || r.get("oID") == null || r.rowCount() <= 0) {
			return null;
		}
		
		// Return the result
		return r.getStringArray("oID")[0];
	}
	
}
