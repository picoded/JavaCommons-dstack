package picoded.dstack.struct.simple;

// Target test class
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

// Test Case include
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import picoded.core.struct.GenericConvertHashMap;
import picoded.core.conv.StringConv;
// Test depends
import picoded.dstack.*;
import picoded.dstack.jsql.*;
import picoded.dstack.core.CoreStack;
import picoded.dstack.struct.simple.*;

public class StructSimpleStack_test {

	// Test object for reuse
	public CoreStack testObj = null;
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Note that this implementation constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public CoreStack implementationConstructor() {
		return new StructSimpleStack(new GenericConvertHashMap<String,Object>());
	}
	
	// Setup and sanity test
	//-----------------------------------------------------
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
	
	// Get and intialize various data structure objects
	//-----------------------------------------------------

	@Test
	public void testDataObject() {
		DataObjectMap dataObjectMap = testObj.dataObjectMap(JSqlTestConfig.randomTablePrefix());
		dataObjectMap.systemSetup();

		DataObject newEntry = dataObjectMap.newEntry();
		newEntry.put("Testing", "value");
		newEntry.saveAll();

		DataObject getObject = dataObjectMap.get(newEntry._oid());
		assertEquals("value", getObject.getString("Testing"));
	}

	@Test
	public void testFileWorkspaceMap(){
		FileWorkspaceMap fileWorkspaceMap = testObj.fileWorkspaceMap(JSqlTestConfig.randomTablePrefix());
		fileWorkspaceMap.systemSetup();

		FileWorkspace newEntry = fileWorkspaceMap.newEntry();
		newEntry.writeByteArray("testing path", "testing value".getBytes());

		FileWorkspace getObject = fileWorkspaceMap.get(newEntry._oid());
		assertEquals("testing value", StringConv.fromByteArray(getObject.readByteArray("testing path")));
	}
	
	@Test
	public void testKeyLongMap(){
		KeyLongMap keyLongMap = testObj.keyLongMap(JSqlTestConfig.randomTablePrefix());
		keyLongMap.systemSetup();

		keyLongMap.putLong("testing", 5L);
		assertEquals(5L, keyLongMap.getLong("testing"));
	}

	@Test
	public void testKeyValueMap(){
		KeyValueMap keyValueMap = testObj.keyValueMap(JSqlTestConfig.randomTablePrefix());
		keyValueMap.systemSetup();

		keyValueMap.put("testing", "value");
		assertEquals("value", keyValueMap.getString("testing"));
	}
}
