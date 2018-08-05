package picoded.dstack.file.simple;

import picoded.dstack.core.Core_FileWorkspaceMap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Reference class for Core_FileWorkspaceMap
 * Provide Crud operation for fileWorkspace
 */
public class  File_FileWorkspaceMap extends Core_FileWorkspaceMap {

	protected Map<String, Map<String, File>> workspaceMap;
	protected ReentrantReadWriteLock accessLock = new ReentrantReadWriteLock();


	@Override
	public void backend_workspaceRemove(String oid) {

	}

	/**
	 * Check if a workspace exists or not
	 * @param oid
	 * @return
	 */
	@Override
	public boolean backend_workspaceExist(String oid) {
		boolean isExist = false;
		try {
			accessLock.readLock().lock();
			Map<String, File> fileMap = workspaceMap.get(oid);
			if (fileMap.size() > 0) {
				isExist = true;
			}
		}
		finally {
			accessLock.readLock().unlock();
		}
		return isExist;
	}

	/**
	 * Read file content from its path
	 * @param oid
	 * @param  filepath to use for the workspace
	 *
	 * @return
	 */
	@Override
	public byte[] backend_fileRead(String oid, String filepath){
		byte[] fileContent = null;
		try {
			accessLock.readLock().lock();
			Map<String, File> fileMap = workspaceMap.get(oid);

			if (fileMap != null && filepath != null) {
				File file = fileMap.get(filepath);
				fileContent = Files.readAllBytes(file.toPath());
			}
		}
		catch (IOException e) {
			return null;
		}
		finally {
			accessLock.readLock().unlock();
		}
		return fileContent;
	}

	/**
	 * Write into a file, if does not exist create one
	 * @param oid
	 * @param   filepath to use for the workspace
	 * @param   data to write the file with
	 */
	@Override
	public void backend_fileWrite(String oid, String filepath, byte[] data) {
		try {
			accessLock.writeLock().lock();
			Map<String, File> fileMap = (workspaceMap.get(oid) == null) ? new HashMap<>() : workspaceMap.get(oid);
			Path newFilePath = Files.write(Paths.get(filepath), data);

			fileMap.put(filepath, new File(newFilePath.toString()));

			workspaceMap.put(oid, fileMap);
		}
		catch (IOException ioException) {
		}
		finally {
			accessLock.writeLock().unlock();
		}
	}

	/**
	 * Remove a file from the workspace by its id
	 * @param oid identifier to the workspace
	 * @param filepath the file to be removed
	 */
	@Override
	public void backend_removeFile(String oid, String filepath) {
		try {
			accessLock.readLock().lock();
			Map<String, File> toRemoveFileMap = workspaceMap.get(oid);
			File file = toRemoveFileMap.get(filepath);
			if (toRemoveFileMap != null && file != null) {
				file.deleteOnExit();
				file.delete();
			}
		}
		finally {
			accessLock.readLock().unlock();
		}
	}

	@Override
	public void systemSetup() {

	}

	@Override
	public void systemDestroy() {
		clear();
	}

	/**
	 * WIpe out the entire fileWorkspace map
	 */
	@Override
	public void clear() {
		try {
			accessLock.writeLock().lock();
			workspaceMap.clear();
		} finally {
			accessLock.writeLock().unlock();
		}
	}
}