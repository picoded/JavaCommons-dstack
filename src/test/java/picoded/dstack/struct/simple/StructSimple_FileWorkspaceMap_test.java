package picoded.dstack.struct.simple;

// Target test class
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;

// Test Case include
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// Test depends
import picoded.dstack.*;
import picoded.dstack.core.Core_FileWorkspace;
import picoded.dstack.struct.simple.*;

public class StructSimple_FileWorkspaceMap_test {
	
	// Test object for reuse
	public FileWorkspaceMap testObj = null;
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Note that this implementation constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public FileWorkspaceMap implementationConstructor() {
		return new StructSimple_FileWorkspaceMap();
	}
	
	@Before
	public void systemSetup() {
		testObj = implementationConstructor();
		testObj.systemSetup();
	}
	
	@After
	public void systemDestroy() {
		if (testObj != null) {
			testObj.systemDestroy();
		}
		testObj = null;
	}
	
	@Test
	public void constructorSetupAndMaintenance() {
		// not null check
		assertNotNull(testObj);
		
		// run maintaince, no exception?
		testObj.maintenance();
		
		// run incremental maintaince, no exception?
		testObj.incrementalMaintenance();
	}
	
	@Test
	public void createFileWorkspace() {
		// Get a non existant filepath
		assertNull(testObj.get("nonExistence.txt"));
		FileWorkspace fileWorkspace = testObj.newEntry();
		assertNotNull(fileWorkspace);
	}
	
	@Test
	public void workspaceExistence() {
		// Setup new entry with a record
		FileWorkspace fileWorkspace = testObj.newEntry();
		fileWorkspace.writeByteArray("filepath.txt", "anything".getBytes());
		
		// Validate read
		byte[] readArr = fileWorkspace.readByteArray("filepath.txt");
		assertNotNull(readArr);
		assertEquals("anything", new String(readArr));
		
		// Get the new entry oid
		String oid = fileWorkspace._oid();
		assertNotNull(oid);
		
		// Assert it exists
		assertNotNull(testObj.get(oid));
	}
	
	//
	// @TODO : Refactor the test cases below to make more sense for file systems
	//
	
	@Test
	public void checkFileExist() {
		FileWorkspace fileWorkspace = testObj.newEntry();
		assertTrue(!fileWorkspace.fileExist("nonExistence"));
		fileWorkspace.writeByteArray("nonExistence", "".getBytes());
		assertTrue(fileWorkspace.fileExist("nonExistence"));
	}
	
	@Test
	public void writeToFile() {
		FileWorkspace fileWorkspace = testObj.newEntry();
		fileWorkspace.writeByteArray("testPath", "data to write".getBytes());
		assertNotNull(testObj.get(fileWorkspace._oid()).readByteArray("testPath"));
		
		fileWorkspace.writeByteArray("a long string", "some test".getBytes());
		assertNotNull(testObj.get(fileWorkspace._oid()).readByteArray("a long string"));
	}
	
	@Test
	public void readFromFile() {
		FileWorkspace fileWorkspace = testObj.newEntry();
		String content = "this is a sentence";
		fileWorkspace.writeByteArray("reader reading", content.getBytes());
		String actualContent = new String(fileWorkspace.readByteArray("reader reading"));
		assertEquals(content, actualContent);
	}
	
	@Test
	public void readNonExistenceFile() {
		FileWorkspace fileWorkspace = testObj.newEntry();
		assertNull(fileWorkspace.readByteArray("unknown path"));
	}
	
	@Test
	public void deleteExistingFile() {
		FileWorkspace fileWorkspace = testObj.newEntry();
		fileWorkspace.writeByteArray("existingDeletion", "to be deleted".getBytes());
		assertNotNull(fileWorkspace.fileExist("existingDeletion"));
		fileWorkspace.removeFile("existingDeletion");
		assertFalse(fileWorkspace.fileExist("existingDeletion"));
	}
	
	@Test
	public void deleteNonExistingFile() {
		FileWorkspace fileWorkspace = testObj.newEntry();
		assertFalse(fileWorkspace.fileExist("alreadyNotExist"));
		fileWorkspace.removeFile("alreadyNotExist");
		assertFalse(fileWorkspace.fileExist("alreadyNotExist"));
	}
	
	@Test
	public void deleteWorkspace() {
		FileWorkspace fileWorkspace = testObj.newEntry();
		fileWorkspace.writeByteArray("filepath", "anything".getBytes());
		assertNotNull(testObj.get(fileWorkspace._oid()));
		testObj.remove(fileWorkspace._oid());
		assertNull(testObj.get(fileWorkspace._oid()));
	}
}
