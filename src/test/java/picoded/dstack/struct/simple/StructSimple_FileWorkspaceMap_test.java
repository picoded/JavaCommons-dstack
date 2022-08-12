package picoded.dstack.struct.simple;

// Target test class
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
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
	
	//-----------------------------------------------------------------------------------
	//
	// Folder pathing test
	//
	//-----------------------------------------------------------------------------------
	
	@Test
	public void folderSetupAndRemove() {
		// Get the file workspace to use
		FileWorkspace fileWorkspace = testObj.newEntry();
		assertNotNull(fileWorkspace);
		
		// Folder does not exist first
		assertFalse(fileWorkspace.folderPathExist("test/folder"));
		
		// Set it up and assert
		fileWorkspace.ensureFolderPath("test/folder");
		assertTrue(fileWorkspace.folderPathExist("test/folder"));
		
		// Remove and assert
		fileWorkspace.removeFolderPath("test/folder");
		assertFalse(fileWorkspace.folderPathExist("test/folder"));
		assertTrue(fileWorkspace.folderPathExist("test"));
		
	}
	
	@Test
	public void folderSetupAndRemove_pathVarients() {
		// Get the file workspace to use
		FileWorkspace fileWorkspace = testObj.newEntry();
		assertNotNull(fileWorkspace);
		
		// Folder does not exist first
		assertFalse(fileWorkspace.folderPathExist("test/folder"));
		assertFalse(fileWorkspace.folderPathExist("test/folder/"));
		assertFalse(fileWorkspace.folderPathExist("/test/folder/"));
		
		// Set it up and assert
		fileWorkspace.ensureFolderPath("test/folder");
		assertTrue(fileWorkspace.folderPathExist("test/folder"));
		assertTrue(fileWorkspace.folderPathExist("test/folder/"));
		assertTrue(fileWorkspace.folderPathExist("/test/folder"));
		assertTrue(fileWorkspace.folderPathExist("/test/folder/"));
		
		// Remove and assert
		fileWorkspace.removeFolderPath("test/folder");
		assertFalse(fileWorkspace.folderPathExist("test/folder"));
		assertTrue(fileWorkspace.folderPathExist("/test"));
		assertTrue(fileWorkspace.folderPathExist("/test/"));
		
	}
	
	@Test
	public void noFileRead_returnsNull() {
		// Get the file workspace to use
		FileWorkspace fileWorkspace = testObj.newEntry();
		assertNotNull(fileWorkspace);
		
		// Check for null (no file) for non existant file
		assertNull(fileWorkspace.readString("test/folder/file.txt"));
		
		// Write file
		fileWorkspace.writeString("test/folder/file.txt", "anything");
		assertTrue(fileWorkspace.fileExist("test/folder/file.txt"));
		assertEquals(fileWorkspace.readString("test/folder/file.txt"), "anything");
		
		// Check for null (no file) for non existant file
		assertNull(fileWorkspace.readString("test/folder/file-somethingElse.txt"));
	}
	
	@Test
	public void fileWrite_andProperlySetupFolder() {
		// Get the file workspace to use
		FileWorkspace fileWorkspace = testObj.newEntry();
		assertNotNull(fileWorkspace);
		
		// Folder does not exist first
		assertFalse(fileWorkspace.folderPathExist("test/folder"));
		
		// Check for null (no file) for non existant file
		assertNull(fileWorkspace.readString("test/folder/file.txt"));
		
		// Write file
		fileWorkspace.writeString("test/folder/file.txt", "anything");
		assertTrue(fileWorkspace.folderPathExist("test/folder"));
		assertTrue(fileWorkspace.fileExist("test/folder/file.txt"));
		
		// Remove and assert
		fileWorkspace.removeFolderPath("test/folder");
		assertFalse(fileWorkspace.fileExist("test/folder/file.txt"));
		assertFalse(fileWorkspace.folderPathExist("test/folder"));
		
		assertTrue(fileWorkspace.folderPathExist("test"));
	}
	
	//-----------------------------------------------------------------------------------
	//
	// Multiple Writes
	//
	//-----------------------------------------------------------------------------------
	
	@Test
	public void fileWrite_fiveTimes() {
		// Get the file workspace to use
		FileWorkspace fileWorkspace = testObj.newEntry();
		assertNotNull(fileWorkspace);
		
		// Folder does not exist first
		assertFalse(fileWorkspace.folderPathExist("test/folder"));
		
		// Write and read file
		for (int i = 0; i < 5; ++i) {
			fileWorkspace.writeString("test/folder/file.txt", "ver-" + i);
			assertEquals("ver-" + i, fileWorkspace.readString("test/folder/file.txt"));
			fileWorkspace.writeString("test/folder/file.txt", "ver-" + i);
			assertEquals("ver-" + i, fileWorkspace.readString("test/folder/file.txt"));
		}
	}
	
	@Test
	public void fileWrite_twentyTimes() {
		// Get the file workspace to use
		FileWorkspace fileWorkspace = testObj.newEntry();
		assertNotNull(fileWorkspace);
		
		// Folder does not exist first
		assertFalse(fileWorkspace.folderPathExist("test/folder"));
		
		// Write and read file
		for (int i = 0; i < 20; ++i) {
			fileWorkspace.writeString("test/folder/file.txt", "ver-" + i);
			assertEquals("ver-" + i, fileWorkspace.readString("test/folder/file.txt"));
			fileWorkspace.writeString("test/folder/file.txt", "ver-" + i);
			assertEquals("ver-" + i, fileWorkspace.readString("test/folder/file.txt"));
		}
	}
	
	//-----------------------------------------------------------------------------------
	//
	// Move test
	//
	//-----------------------------------------------------------------------------------
	
	@Test
	public void fileWrite_andFileMove() {
		// Get the file workspace to use
		FileWorkspace fileWorkspace = testObj.newEntry();
		assertNotNull(fileWorkspace);
		
		// Write file
		fileWorkspace.writeString("test/folder/file.txt", "anything");
		assertTrue(fileWorkspace.fileExist("test/folder/file.txt"));
		
		// Move it
		fileWorkspace.moveFile("test/folder/file.txt", "test/folder/moved.txt");
		
		// File moved
		assertFalse(fileWorkspace.fileExist("test/folder/file.txt"));
		assertTrue(fileWorkspace.fileExist("test/folder/moved.txt"));
	}
	
	@Test
	public void fileWrite_andFolderMove() {
		// Get the file workspace to use
		FileWorkspace fileWorkspace = testObj.newEntry();
		assertNotNull(fileWorkspace);
		
		// Write file
		fileWorkspace.writeString("test/folder/file.txt", "anything");
		assertTrue(fileWorkspace.fileExist("test/folder/file.txt"));
		
		// Move it
		fileWorkspace.moveFolderPath("test/folder/", "moved/folder/");
		
		// File moved
		assertFalse(fileWorkspace.fileExist("test/folder/file.txt"));
		assertTrue(fileWorkspace.fileExist("moved/folder/file.txt"));
	}
	
	//-----------------------------------------------------------------------------------
	//
	// Copy test
	//
	//-----------------------------------------------------------------------------------
	
	@Test
	public void fileWrite_andFileCopy() {
		// Get the file workspace to use
		FileWorkspace fileWorkspace = testObj.newEntry();
		assertNotNull(fileWorkspace);
		
		// Write file
		fileWorkspace.writeString("test/folder/file.txt", "anything");
		assertTrue(fileWorkspace.fileExist("test/folder/file.txt"));
		
		// Copy it
		fileWorkspace.copyFile("test/folder/file.txt", "test/folder/copied.txt");
		
		// Original file remains intact
		assertTrue(fileWorkspace.fileExist("test/folder/file.txt"));
		
		// File has been copied
		assertTrue(fileWorkspace.fileExist("test/folder/copied.txt"));
		
		// File should be equals
		assertEquals(fileWorkspace.readString("/test/folder/copied.txt"), "anything");
	}
	
	@Test
	public void fileWrite_andFolderCopy() {
		// Get the file workspace to use
		FileWorkspace fileWorkspace = testObj.newEntry();
		assertNotNull(fileWorkspace);
		
		// Write file
		fileWorkspace.writeString("test/folder/file.txt", "anything");
		assertTrue(fileWorkspace.fileExist("test/folder/file.txt"));
		
		// Copy the folder
		fileWorkspace.copyFolderPath("test/folder/", "test/copied/");
		
		// Original folder/file remains intact
		assertTrue(fileWorkspace.fileExist("test/folder/file.txt"));
		
		// Folder has been copied
		assertTrue(fileWorkspace.fileExist("test/copied/file.txt"));
		
		// File should be equals
		assertEquals(fileWorkspace.readString("test/copied/file.txt"), "anything");
	}
	
	//-----------------------------------------------------------------------------------
	//
	// Path sets
	//
	//-----------------------------------------------------------------------------------
	
	@Test
	public void getPathSetLookup() {
		// Get the file workspace to use
		FileWorkspace fileWorkspace = testObj.newEntry();
		assertNotNull(fileWorkspace);
		fileWorkspace.setupWorkspace();
		
		// List blank
		assertEquals(0, fileWorkspace.getFilePathSet("").size());
		assertEquals(0, fileWorkspace.getFolderPathSet("").size());
		assertEquals(0, fileWorkspace.getFileAndFolderPathSet("").size());
		
		// Write stuff
		fileWorkspace.writeString("test/one.txt", "anything");
		fileWorkspace.writeString("test/two.txt", "anything");
		fileWorkspace.writeString("test/d1/file.txt", "anything");
		fileWorkspace.writeString("test/d2/file.txt", "anything");
		
		// List files and folders
		assertEquals(4, fileWorkspace.getFileAndFolderPathSet("test").size());
		assertEquals(2, fileWorkspace.getFilePathSet("test").size());
		assertEquals(2, fileWorkspace.getFolderPathSet("test").size());
	}
	
	//-----------------------------------------------------------------------------------
	//
	// @TODO : Refactor the test cases below to make more sense for file systems
	//
	//-----------------------------------------------------------------------------------
	
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
		try {
			fileWorkspace.readByteArray("unknown path");
		} catch (Exception e) {
			assertEquals("File does not exist.", e.getMessage());
		}
		
	}
	
	@Test
	public void writeAndReadToFile_stream() throws Exception {
		// Output stream to use for content
		ByteArrayInputStream buffer = new ByteArrayInputStream("data to write".getBytes());
		
		FileWorkspace fileWorkspace = testObj.newEntry();
		fileWorkspace.writeInputStream("testPath", buffer);
		assertNotNull(testObj.get(fileWorkspace._oid()).readByteArray("testPath"));
		
		InputStream readData = testObj.get(fileWorkspace._oid()).readInputStream("testPath");
		byte[] readArray = IOUtils.toByteArray(readData);
		assertEquals(new String(readArray), "data to write");
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


	@Test
	public void keySetTest() {
		FileWorkspace fileWorkspace = testObj.newEntry();
		fileWorkspace.writeByteArray("filepath", "anything".getBytes());

		// Get the workspace keyset
		Set<String> keyset = testObj.keySet();
		assertNotNull(keyset);
		assertTrue( keyset.contains(fileWorkspace._oid()) );
	}
}
