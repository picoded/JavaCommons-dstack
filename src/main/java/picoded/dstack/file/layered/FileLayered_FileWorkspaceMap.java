package picoded.dstack.file.layered;

import picoded.dstack.file.simple.FileSimple_FileWorkspaceMap;

import java.io.File;

public class FileLayered_FileWorkspaceMap extends FileSimple_FileWorkspaceMap {
	
	//--------------------------------------------------------------------------
	//
	// Constructor vars
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Setup with file directory
	 *
	 * @param inDir folder directory to operate from
	 */
	public FileLayered_FileWorkspaceMap(File inDir) {
		super(inDir);
	}
	
	/**
	 * Setup with file directory
	 *
	 * @param inDir folder directory to operate from
	 */
	public FileLayered_FileWorkspaceMap(String inDir) {
		super(inDir);
	}
	
	/**
	 * Get and return the workspace file object
	 * To be used for subsequent operations
	 *
	 * @return file object
	 */
	@Override
	public File workspaceDirObj(String oid) {
		// oid failed validation
		if (!validateOid(oid)) {
			return null;
		}
		
		// Make it into a layered directory
		String workspacePath = oid.substring(0, 2) + "/" + oid.substring(2, 4) + "/" + oid
			+ "/workspace";
		
		// Get the file directory
		return new File(baseDir, workspacePath);
	}
	
}
