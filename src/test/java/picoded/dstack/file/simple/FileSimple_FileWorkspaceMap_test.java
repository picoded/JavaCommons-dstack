package picoded.dstack.file.simple;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

// Test Case include
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// Test depends
import picoded.core.file.FileUtil;
import picoded.dstack.*;
import picoded.dstack.struct.simple.*;

public class FileSimple_FileWorkspaceMap_test extends StructSimple_FileWorkspaceMap_test {
	
	// To override for implementation
	//-----------------------------------------------------
	
	// Test directory to use
	File testWorkspaceDir = null;
	
	/// Note that this implementation constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public FileWorkspaceMap implementationConstructor() {
		return new FileSimple_FileWorkspaceMap(testWorkspaceDir);
	}
	
	@Before
	public void systemSetup() {
		try {
			// Setup dir
			Path tempDirPath = Files.createTempDirectory("TEST-FileSimple-FileWorkspaceMap");
			testWorkspaceDir = tempDirPath.toFile();
			testWorkspaceDir.deleteOnExit();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		// Does the rest of test setup
		super.systemSetup();
	}
	
	@After
	public void systemDestroy() {
		// Does the typical test destroy steps
		super.systemDestroy();
		
		// Delete the directory if failed to cleanup
		if (testWorkspaceDir.isDirectory()) {
			FileUtil.forceDelete(testWorkspaceDir);
		}
		
		// Set directory to null
		testWorkspaceDir = null;
	}
	
}
