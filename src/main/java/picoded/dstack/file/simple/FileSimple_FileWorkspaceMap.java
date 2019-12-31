package picoded.dstack.file.simple;

import picoded.core.file.FileUtil;
import picoded.dstack.core.Core_FileWorkspaceMap;

import java.io.File;
import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.management.RuntimeErrorException;

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
	 * @param inDir folder directory to operate from
	 */
	public FileSimple_FileWorkspaceMap(File inDir) {
		baseDir = inDir;
		validateBaseDir();
	}
	
	/**
	 * Setup with file directory
	 *
	 * @param inDir folder directory to operate from
	 */
	public FileSimple_FileWorkspaceMap(String inDir) {
		baseDir = new File(inDir);
		validateBaseDir();
	}
	
	//--------------------------------------------------------------------------
	//
	// Workspace
	//
	//--------------------------------------------------------------------------
	
	protected void validateBaseDir() {
		if (!baseDir.exists() || !baseDir.isDirectory()) {
			throw new RuntimeException("Missing storage directory for workspace. Path: "
				+ baseDir.getAbsolutePath());
		}
	}
	
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
	public File workspaceDirObj(String oid) {
		// oid failed validation
		if (!validateOid(oid)) {
			return null;
		}
		
		// Get the file directory
		return new File(baseDir, oid + "/workspace/");
	}
	
	/**
	 * Checks and return of a workspace exists
	 *
	 * @param Object ID of workspace to get
	 * @return boolean to check if workspace exists
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
	
	/**
	 * Setup the current fileWorkspace within the fileWorkspaceMap,
	 * <p>
	 * This ensures the workspace _oid is registered within the map,
	 * even if there is 0 files.
	 * <p>
	 * Does not throw any error if workspace was previously setup
	 */
	@Override
	public void backend_setupWorkspace(String oid) {
		File file = workspaceDirObj(oid);
		if (file == null) {
			throw new RuntimeException("Invalid OID (unable to setup)");
		}
		boolean mkdir = file.mkdirs();
	}
	
	//--------------------------------------------------------------------------
	//
	// File handling
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Get and return the file object, given the oid and path
	 *
	 * @param ObjectID of workspace
	 * @param filepath to use for the workspace
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
	 *
	 * @param oid
	 * @param filepath to use for the workspace
	 * @return
	 */
	@Override
	public byte[] backend_fileRead(String oid, String filepath) {
		// Get the file object
		File fileObj = workspaceFileObj(oid, filepath);
		
		// return null if failed
		if (fileObj == null || !fileObj.exists()) {
			return null;
		}
		
		if (fileObj.isDirectory()) {
			throw new RuntimeException(String.format("`%s` is a directory", filepath));
		}
		
		// Read the file
		return FileUtil.readFileToByteArray(fileObj);
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
		File fileObj = workspaceFileObj(oid, filepath);
		return fileObj.isFile();
	}
	
	/**
	 * Write into a file, if does not exist createUser one
	 *
	 * @param oid
	 * @param filepath to use for the workspace
	 * @param data     to write the file with
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
	 *
	 * @param oid      identifier to the workspace
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
			FileUtil.deleteDirectory(baseDir);
			// Ensure the base directory is initialized
			FileUtil.forceMkdir(baseDir);
		}
	}
	
	//--------------------------------------------------------------------------
	//
	// Folder handling
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
		File folderObj = workspaceFileObj(oid, folderPath);
		FileUtil.deleteDirectory(folderObj);
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
		File folderObj = workspaceFileObj(oid, folderPath);
		return folderObj.isDirectory();
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
		File folderObj = workspaceFileObj(oid, folderPath);
		FileUtil.forceMkdir(folderObj);
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
		try {
			File fileObj = workspaceFileObj(oid, filepath);
			BasicFileAttributes attr = Files.readAttributes(fileObj.toPath(), BasicFileAttributes.class);
			return attr.creationTime().toMillis();
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
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
		File fileObj = workspaceFileObj(oid, filepath);
		return fileObj.lastModified();
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
		
		// Check for source file
		File sourceObj = workspaceFileObj(oid, sourceFile);
		if (!sourceObj.isFile()) {
			throw new RuntimeException("sourceFile does not exist / is not a file (oid=" + oid
				+ ") : " + sourceFile);
		}
		
		// Apply the move
		FileUtil.moveFile(sourceObj, workspaceFileObj(oid, destinationFile));
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
		// Check for source folder
		File sourceObj = workspaceFileObj(oid, sourceFolder);
		if (!sourceObj.isDirectory()) {
			throw new RuntimeException("sourceFolder does not exist / is not a folder (oid=" + oid
				+ ") : " + sourceFolder);
		}
		
		// Apply the move
		FileUtil.moveDirectory(sourceObj, workspaceFileObj(oid, destinationFolder));
	}
	
	//--------------------------------------------------------------------------
	//
	// Listing support
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Recusrively iterate internally, a given directory and populate the result set of valid paths
	 * 
	 * @param result
	 * @param currentDir
	 * @param currentPath
	 * @param maxDepth
	 */
	protected void recusively_populatePathSet(Set<String> result, File currentDir,
		String currentPath, int maxDepth) {
		// List all the files
		File[] fileList = currentDir.listFiles();
		
		// For each one of it, process it!
		for (File subFile : fileList) {
			// Get subFile name
			String subFileName = subFile.getName();
			
			// If its a file - add it, and move on
			if (subFile.isFile()) {
				result.add(currentPath + subFileName);
				continue;
			}
			
			// Not a file? huh - skip
			if (!subFile.isDirectory()) {
				continue;
			}
			
			// Ok safely assume a directory
			String subDirectoryPath = currentPath + subFileName + "/";
			result.add(subDirectoryPath);
			
			// Lets recursively look into it (if needed)
			if (maxDepth <= -1) {
				// No max depth check - dive in
				recusively_populatePathSet(result, subFile, subDirectoryPath, -1);
			} else if (maxDepth == 0) {
				// End of recursion - abort
				continue;
			} else {
				// Time to loop and count it
				recusively_populatePathSet(result, subFile, subDirectoryPath, maxDepth - 1);
			}
		}
	}
	
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
		// Check for source folder
		File folderObj = workspaceFileObj(oid, folderPath);
		if (!folderObj.isDirectory()) {
			throw new RuntimeException("folderPath does not exist / is not a folder (oid=" + oid
				+ ") : " + folderPath);
		}
		
		// Prepare the return set
		Set<String> retSet = new HashSet<>();
		
		// Lets recursively loop through the files
		recusively_populatePathSet(retSet, folderObj, "", maxDepth);
		
		// Return with minDepth filtering
		return backend_filtterPathSet(retSet, "", minDepth, maxDepth, 0);
	}
	
}
