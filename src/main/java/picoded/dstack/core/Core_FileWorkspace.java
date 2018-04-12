package picoded.dstack.core;

// Java imports
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// Picoded imports
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

			if(_oid.length() < 4) {
				throw new RuntimeException("_oid should be atleast 4 character long");
			}
		}
		
		// _oid setup
		_oid = inOID;
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
		return main.backend_fileExist(_oid, filepath);
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
		if( read == null ) {
			writeByteArray(filepath, data);
		}

		// Append new data to existing data
		byte[] jointData = ArrayConv.addAll(read, data);

		// Write the new joint data
		writeByteArray(filepath, jointData);
	}


}