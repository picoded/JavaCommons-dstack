package picoded.dstack.jsql;

import picoded.dstack.core.Core_FileWorkspaceMap;
import picoded.dstack.connector.jsql.JSql;
import picoded.dstack.connector.jsql.JSqlResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

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
	 * Raw datastorage type
	 **/
	protected String rawDataColumnType = "BLOB";
	
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
			new Object[] { oid });
		return jSqlResult.rowCount() > 0;
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
		JSqlResult jSqlResult = sqlObj.select(fileWorkspaceTableName, null, "oID = ? AND path = ?",
			new Object[] { oid, filepath });
		
		if (jSqlResult == null || jSqlResult.get("data") == null || jSqlResult.rowCount() <= 0) {
			return null;
		}
		return (byte[]) jSqlResult.get("data").get(0);
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
		long now = JSql_DataObjectMapUtil.getCurrentTimestamp();
		sqlObj.upsert( //
			fileWorkspaceTableName, //
			new String[] { "oID", "path" }, //
			new Object[] { oid, filepath }, //
			new String[] { "uTm" }, //
			new Object[] { now }, //
			new String[] { "cTm", "eTm", "data" }, //
			new Object[] { now, 0, data }, //
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
	
	/**
	 * Setup the current fileWorkspace within the fileWorkspaceMap,
	 *
	 * This ensures the workspace _oid is registered within the map,
	 * even if there is 0 files.
	 *
	 * Does not throw any error if workspace was previously setup
	 */
	@Override
	public void backend_setupWorkspace(String oid, String folderPath) {
		// Setup a blank folder path
		long now = JSql_DataObjectMapUtil.getCurrentTimestamp();
		sqlObj.upsert( //
			fileWorkspaceTableName, //
			new String[] { "oID", "path" }, //
			new Object[] { oid, folderPath }, //
			new String[] { "uTm" }, //
			new Object[] { now }, //
			new String[] { "cTm", "eTm", "data" }, //
			new Object[] { now, 0, null }, //
			null // The only misc col, is pKy, which is being handled by DB
			);
	}
	
	@Override
	public List<Object> backend_listWorkspace(String oid, String folderPath) {
		// @TODO: To be implemented for Jsql
		//		JSqlResult sqlResult = sqlObj.select(fileWorkspaceTableName, "*","path LIKE ?", new Object[]{folderPath+"%"});
		return new ArrayList<>();
	}
	
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
}
