package picoded.dstack.file.layered;

import picoded.dstack.file.simple.FileSimple_FileWorkspaceMap;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

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
	
	//--------------------------------------------------------------------------
	//
	// KeySet support
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Get and returns all the GUID's, note that due to its
	 * potential of returning a large data set, production use
	 * should be avoided.
	 *
	 * @return set of keys
	 **/
	@Override
	public Set<String> keySet() {
		// The return hashset
		HashSet<String> ret = new HashSet<String>();
		
		// List all the files/folders
		File[] l1_dirList = baseDir.listFiles();
		
		// For each one of it, process it!
		for (File l1_dir : l1_dirList) {
			// Skip if its not a directory
			if( !l1_dir.isDirectory() ) {
				continue;
			}

			// List all the files/folders
			File[] l2_dirList = baseDir.listFiles();
			
			for(File l2_dir : l2_dirList) {
				// Skip if its not a directory
				if( !l2_dir.isDirectory() ) {
					continue;
				}

				// Get the oidDirLIst
				File[] oid_list = l2_dir.listFiles();

				// For each oid dir
				for(File oid_dir : oid_list) {
					// Get the presumed oid
					String oid = oid_dir.getName();

					// Validate the dir name (oid)
					if( !validateOid(oid) ) {
						continue;
					}

					// Add the oid to the ret set
					ret.add(oid);
				}
			}
		}

		// Return the full keyset
		return ret;
	}
	
}
