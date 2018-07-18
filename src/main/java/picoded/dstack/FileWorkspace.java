package picoded.dstack;

import picoded.core.conv.ArrayConv;

/**
 * Represent a file storage backend for a workspace
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
	
	// File exists checks
	//--------------------------------------------------------------------------
	
	/**
	 * Checks if the filepath exists with a file.
	 * 
	 * @param  filepath in the workspace to check
	 * 
	 * @return true, if file exists (and writable), false if it does not. Possible a folder 
	 */
	boolean fileExist(final String filepath);
	
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
	 * Delete an existing file from the workspace
	 *
	 * @param filepath in the workspace to delete
	 */
	void removeFile(final String filepath);
	
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
	
	// @TODO - once this API is more stable
	//
	// + String handling
	// + File copies within workspace
	// + File moving within workspace
	// + Folder deletion
	// + Folder listing
	//--------------------------------------------------------------------------
	
}
