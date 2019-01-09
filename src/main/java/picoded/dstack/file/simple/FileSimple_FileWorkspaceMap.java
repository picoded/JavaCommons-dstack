package picoded.dstack.file.simple;

import picoded.core.file.FileUtil;
import picoded.dstack.FileNode;
import picoded.dstack.core.Core_FileWorkspaceMap;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
			// Note I intentionally did not leak the configured storage path
			// for security reasons. =/
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
		return new File(baseDir, oid);
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
	public void backend_setupWorkspace(String oid, String folderPath) {
		File file = null;
		if (folderPath.isEmpty()) {
			file = new File(baseDir + "/" + oid);
		} else {
			file = new File(baseDir + "/" + oid + "/" + folderPath);
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
		
		// Check if its a file, return null if failed
		if (fileObj == null || !fileObj.isFile()) {
			return null;
		}
		
		// Read the file
		return FileUtil.readFileToByteArray(fileObj);
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
	
	/**
	 * The actual implementation to be completed in the subsequent classes that extends from Core_FileWorkspaceMap.
	 * List the files and folder recursively depending on the folderPath that was passed in.
	 * The Object in this implementation is a Map<String, Object>.
	 *
	 * @param oid        of the workspace to search
	 * @param folderPath start of the folderPath to retrieve from
	 * @param depth      the level of recursion that this is going to go to, -1 will be listing all the way
	 * @return back a list of Objects in tree view
	 */
	@Override
	public FileNode backend_listWorkspaceTreeView(String oid, String folderPath, int depth) {
		
		File node = workspaceFileObj(oid, folderPath);
		File rootFolder = workspaceDirObj(oid);
		
		FileNode fileNode = new FileSimple_FileNode(node.getName(), node.isDirectory());
		
		// File processing
		//----------------------
		if (node.isFile()) {
			return fileNode;
		}
		
		// Folder processing
		//----------------------
		if (node.isDirectory()) {
			File[] subFiles = node.listFiles();
			
			// Go deeper if depth is not 0 yet or depth is -1 (list all the way)
			if (subFiles != null && subFiles.length > 0 && (depth != 0 || depth == -1)) {
				
				// Calculating the next depth
				depth = (depth == -1) ? -1 : depth - 1;
				
				for (File fileItem : subFiles) {
					
					// Get the inner level files and folder
					FileNode fileItemNode = backend_listWorkspaceTreeView(oid, fileItem
						.getAbsolutePath().replace(rootFolder.getAbsolutePath(), ""), depth);
					
					if (fileItemNode != null) {
						fileNode.add(fileItemNode);
					}
				}
			}
			
			return fileNode;
		}
		
		throw new IllegalArgumentException("Unexpected file/folder type - " + node.getPath());
		
	}
	
	@Override
	public List<FileNode> backend_listWorkspaceListView(String oid, String folderPath, int depth) {
		File node = workspaceFileObj(oid, folderPath);
		File rootFolder = workspaceDirObj(oid);
		
		try {
			
			Stream<Path> pathStream;
			
			if (depth == -1) {
				pathStream = Files.walk(node.toPath());
			} else {
				pathStream = Files.walk(node.toPath(), depth, FileVisitOption.FOLLOW_LINKS);
			}
			
			List<FileNode> fileNodes = pathStream.map(path -> {
				File pathFile = path.toFile();
				String name = pathFile.getAbsolutePath().replace(rootFolder.getAbsolutePath(), "");
				FileNode fileNode = new FileSimple_FileNode(name, pathFile.isDirectory());
				fileNode.removeChildrenNodes();
				return name == null ? null : fileNode;
			}).collect(Collectors.toList());
			
			fileNodes.removeIf(item -> item == null);
			
			return fileNodes;
			
		} catch (IOException e) {
			throw new RuntimeException(String.format(
				"Unable to walk through folderPath: %s of workspace ID: %s", folderPath, oid));
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
