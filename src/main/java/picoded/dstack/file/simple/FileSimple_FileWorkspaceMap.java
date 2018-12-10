package picoded.dstack.file.simple;

import java.io.File;

import picoded.dstack.core.Core_FileWorkspaceMap;
import picoded.core.file.FileUtil;

/**
 * Reference class for Core_FileWorkspaceMap
 * Provide Crud operation backed by actual files
 */
public class FileSimple_FileWorkspaceMap extends Core_FileWorkspaceMap {
	
	//--------------------------------------------------------------------------
	//
	// Constructor vars
	//
	//--------------------------------------------------------------------------
	
	/// The file directory to opreate from
	protected File baseDir = null;
	
	/// The file suffix to use for JSON object records
	
	/**
	 * Setup with file directory
	 *
	 * @param  inDir folder directory to operate from
	 */
	public FileSimple_FileWorkspaceMap(File inDir) {
		baseDir = inDir;
	}
	
	/**
	 * Setup with file directory
	 *
	 * @param  inDir folder directory to operate from
	 */
	public FileSimple_FileWorkspaceMap(String inDir) {
		baseDir = new File(inDir);
	}
	
	//--------------------------------------------------------------------------
	//
	// Workspace
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Small utility function used to validate oid,
	 * to prevent file pathing attacks
	 *
	 * @return true, if oid pass basic security validations
	 */
	protected boolean validateOid(String oid) {
		// oid is null / invalid
		if (oid == null || oid.length() <= 0) {
			return false;
		}
		
		// Adding safety check for file operation, ensuring oid is alphanumeric
		if (!oid.matches("[a-zA-Z0-9]+")) {
			return false;
		}
		
		// All checks pass
		return true;
	}
	
	/**
	 * Get and return the workspace file object
	 * To be used for subsequent operations
	 *
	 * @return file object
	 */
	protected File workspaceDirObj(String oid) {
		// oid failed validation
		if (!validateOid(oid)) {
			return null;
		}
		
		// Get the file directory
		return new File(baseDir, oid);
	}
	
	/**
	 * Checks and return of a workspace exists
	 *
	 * @param  Object ID of workspace to get
	 *
	 * @return  boolean to check if workspace exists
	 **/
	@Override
	public boolean backend_workspaceExist(String oid) {
		// Workspace directory file
		File workspaceDir = workspaceDirObj(oid);
		
		// Invalid workspace format
		if (workspaceDir == null) {
			return false;
		}
		
		// Validate that the workspace directory is initialized
		return workspaceDir.isDirectory();
	}
	
	/**
	 * Removes the FileWorkspace, used to nuke an entire workspace
	 *
	 * @param ObjectID of workspace to remove
	 **/
	@Override
	public void backend_workspaceRemove(String oid) {
		// Workspace directory file
		File workspaceDir = workspaceDirObj(oid);
		
		// Remove workspace if found valid
		if (workspaceDir != null && workspaceDir.exists()) {
			FileUtil.forceDelete(workspaceDir);
		}
	}
	
	//--------------------------------------------------------------------------
	//
	// File handling
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Get and return the file object, given the oid and path
	 *
	 * @param  ObjectID of workspace
	 * @param  filepath to use for the workspace
	 *
	 * @return file object
	 */
	protected File workspaceFileObj(String oid, String filepath) {
		// Get workspace dir
		File workspaceDir = workspaceDirObj(oid);
		
		// // Return null on failure
		// if (workspaceDir == null) {
		// 	return null;
		// }
		
		// // Normalize filepath, and validate it
		// filepath = FileUtil.normalize(filepath);
		// if (filepath == null) {
		// 	return null;
		// }
		
		// // Get without starting "/"
		// if (filepath.startsWith("/")) {
		// 	filepath = filepath.substring(1);
		// }
		
		// Get the file object
		return new File(workspaceDir, filepath);
	}
	
	/**
	 * Read file content from its path
	 * @param oid
	 * @param  filepath to use for the workspace
	 *
	 * @return
	 */
	@Override
	public byte[] backend_fileRead(String oid, String filepath) {
		// Get the file object
		File fileObj = workspaceFileObj(oid, filepath);
		
		// Check if its a file, return null if failed
		if (fileObj == null || !fileObj.isFile()) {
			return null;
		}
		
		// Read the file
		return FileUtil.readFileToByteArray(fileObj);
	}
	
	/**
	 * Write into a file, if does not exist create one
	 * @param oid
	 * @param   filepath to use for the workspace
	 * @param   data to write the file with
	 */
	@Override
	public void backend_fileWrite(String oid, String filepath, byte[] data) {
		// Get the file object
		File fileObj = workspaceFileObj(oid, filepath);
		
		// Invalid file path?
		if (fileObj == null) {
			return;
		}
		
		// Check if its a file exist,
		// if it doesnt ensure parent folder is intialized
		if (!fileObj.exists()) {
			File parentFile = fileObj.getParentFile();
			FileUtil.forceMkdir(parentFile);
		}
		
		// Write the file
		FileUtil.writeByteArrayToFile(fileObj, data);
	}
	
	/**
	 * Remove a file from the workspace by its id
	 * @param oid identifier to the workspace
	 * @param filepath the file to be removed
	 */
	@Override
	public void backend_removeFile(String oid, String filepath) {
		// Get the file object
		File fileObj = workspaceFileObj(oid, filepath);
		
		// Invalid file path?
		if (fileObj == null) {
			return;
		}
		
		// Check if its a file exist, and delete it
		if (fileObj.isFile()) {
			FileUtil.forceDelete(fileObj);
		}
	}
	
	@Override
	public void systemSetup() {
		if (!baseDir.exists()) {
			// Ensure the base directory is initialized
			FileUtil.forceMkdir(baseDir);
		}
	}
	
	@Override
	public void systemDestroy() {
		
	}
	
	/**
	 * Wipe out the entire fileWorkspace map
	 */
	@Override
	public void clear() {
		if (baseDir.isDirectory()) {
			// Delete the directory, and reinitialize it as empty
			FileUtil.forceDelete(baseDir);
			// Ensure the base directory is initialized
			FileUtil.forceMkdir(baseDir);
		}
	}
}
