package picoded.dstack.struct.simple;

// Target test class
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

// Test Case include
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import picoded.core.struct.GenericConvertHashMap;
import picoded.core.conv.StringConv;
import picoded.core.conv.ConvertJSON;
// Test depends
import picoded.dstack.*;
import picoded.dstack.jsql.*;
import picoded.dstack.core.CoreStack;
import picoded.dstack.struct.simple.*;

public class StructSimpleStack_test {
	
	// Test object for reuse
	public CoreStack testObj = null;
	
	public String tablePrefix = "";
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Note that this implementation constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public CoreStack implementationConstructor() {
		return new StructSimpleStack(new GenericConvertHashMap<String, Object>());
	}
	
	// Setup and sanity test
	//-----------------------------------------------------
	@Before
	public void systemSetup() {
		tablePrefix = retrieveTablePrefix();
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
	
	public String retrieveTablePrefix() {
		return JSqlTestConfig.randomTablePrefix();
	}
	
	// Get and intialize various data structure objects
	//-----------------------------------------------------
	
	@Test
	public void test_retrieveDataObject() {
		DataObjectMap dataObjectMap = testObj.dataObjectMap(tablePrefix);
		dataObjectMap.systemSetup();
		
		DataObject newEntry = dataObjectMap.newEntry();
		newEntry.put("Testing", "value");
		newEntry.saveAll();
		
		DataObject getObject = dataObjectMap.get(newEntry._oid());
		assertEquals("value", getObject.getString("Testing"));
	}
	
	@Test
	public void test_retrieveFileWorkspaceMap() {
		FileWorkspaceMap fileWorkspaceMap = testObj.fileWorkspaceMap(tablePrefix);
		fileWorkspaceMap.systemSetup();
		
		FileWorkspace newEntry = fileWorkspaceMap.newEntry();
		newEntry.writeByteArray("testing path", "testing value".getBytes());
		
		FileWorkspace getObject = fileWorkspaceMap.get(newEntry._oid());
		assertEquals("testing value",
			StringConv.fromByteArray(getObject.readByteArray("testing path")));
	}
	
	@Test
	public void test_retrieveKeyLongMap() {
		KeyLongMap keyLongMap = testObj.keyLongMap(tablePrefix);
		keyLongMap.systemSetup();
		
		keyLongMap.putLong("testing", 5L);
		assertEquals(5L, keyLongMap.getLong("testing"));
	}
	
	@Test
	public void test_retrieveKeyValueMap() {
		KeyValueMap keyValueMap = testObj.keyValueMap(tablePrefix);
		keyValueMap.systemSetup();
		
		keyValueMap.put("testing", "value");
		assertEquals("value", keyValueMap.getString("testing"));
	}
	
	// Delete operations for various data structure objects
	//-----------------------------------------------------
	
	@Test
	public void test_deleteDataObject() {
		DataObjectMap dataObjectMap = testObj.dataObjectMap(tablePrefix);
		dataObjectMap.systemSetup();
		
		DataObject newEntry = dataObjectMap.newEntry();
		newEntry.put("Testing", "value");
		newEntry.saveAll();
		
		DataObject getObject = dataObjectMap.get(newEntry._oid());
		assertEquals("value", getObject.getString("Testing"));
		
		dataObjectMap.remove(getObject._oid());
		
		getObject = dataObjectMap.get(newEntry._oid());
		assertNull(getObject);
	}
	
	@Test
	public void test_deleteFileWorkspace() {
		FileWorkspaceMap fileWorkspaceMap = testObj.fileWorkspaceMap(tablePrefix);
		fileWorkspaceMap.systemSetup();
		
		FileWorkspace newEntry = fileWorkspaceMap.newEntry();
		newEntry.writeByteArray("testing path", "testing value".getBytes());
		
		FileWorkspace getObject = fileWorkspaceMap.get(newEntry._oid());
		assertEquals("testing value",
			StringConv.fromByteArray(getObject.readByteArray("testing path")));
		
		fileWorkspaceMap.remove(getObject._oid());
		
		getObject = fileWorkspaceMap.get(newEntry._oid());
		assertNull(getObject);
	}
	
	@Test
	public void test_deleteKeyLong() {
		KeyLongMap keyLongMap = testObj.keyLongMap(tablePrefix);
		keyLongMap.systemSetup();
		
		keyLongMap.putLong("testing", 5L);
		assertEquals(5L, keyLongMap.getLong("testing"));
		
		keyLongMap.remove("testing");
		assertEquals(0, keyLongMap.getLong("testing"));
	}
	
	@Test
	public void test_deleteKeyValue() {
		KeyValueMap keyValueMap = testObj.keyValueMap(tablePrefix);
		keyValueMap.systemSetup();
		
		keyValueMap.put("testing", "value");
		assertEquals("value", keyValueMap.getString("testing"));
		
		keyValueMap.remove("testing");
		assertEquals(null, keyValueMap.getString("testing"));
	}
	
	// Expiry and lifespan operations for various data structure objects
	//-----------------------------------------------------
	
	@Test
	public void test_expiryKeyLong() throws Exception {
		KeyLongMap keyLongMap = testObj.keyLongMap(tablePrefix);
		keyLongMap.systemSetup();
		
		long now = System.currentTimeMillis();
		keyLongMap.putWithExpiry("testing", 5L, now + 300);
		assertEquals(5L, keyLongMap.getLong("testing"));
		
		Thread.sleep(300);
		assertEquals(0, keyLongMap.getLong("testing"));
	}
	
	@Test
	public void test_expiryKeyValue() throws Exception {
		KeyValueMap keyValueMap = testObj.keyValueMap(tablePrefix);
		keyValueMap.systemSetup();
		
		long now = System.currentTimeMillis();
		keyValueMap.putWithExpiry("testing", "value", now + 300);
		assertEquals("value", keyValueMap.getString("testing"));
		
		Thread.sleep(300);
		assertEquals(null, keyValueMap.getString("testing"));
	}
	
	@Test
	public void test_lifeSpanKeyLong() throws Exception {
		KeyLongMap keyLongMap = testObj.keyLongMap(tablePrefix);
		keyLongMap.systemSetup();
		
		long now = System.currentTimeMillis();
		keyLongMap.putWithLifespan("testing", 5L, 300);
		assertEquals(5L, keyLongMap.getLong("testing"));
		
		Thread.sleep(300);
		assertEquals(0, keyLongMap.getLong("testing"));
	}
	
	@Test
	public void test_lifeSpanKeyValue() throws Exception {
		KeyValueMap keyValueMap = testObj.keyValueMap(tablePrefix);
		keyValueMap.systemSetup();
		
		long now = System.currentTimeMillis();
		keyValueMap.putWithLifespan("testing", "value", 300);
		assertEquals("value", keyValueMap.getString("testing"));
		
		Thread.sleep(300);
		assertEquals(null, keyValueMap.getString("testing"));
	}
	
	// keySet operations for various data structure objects
	//-----------------------------------------------------
	
	@Test
	public void test_keySetDataObjectMap() {
		DataObjectMap dataObjectMap = testObj.dataObjectMap(tablePrefix);
		dataObjectMap.systemSetup();
		
		DataObject newEntry = dataObjectMap.newEntry();
		newEntry.put("Testing", "value");
		newEntry.saveAll();
		
		List<String> keys = new ArrayList<>();
		keys.add(newEntry._oid());
		for (String key : dataObjectMap.keySet()) {
			keys.remove(key);
		}
		assertTrue(keys.size() == 0);
	}
	
	@Test
	public void test_keySetKeyLongMap() {
		KeyLongMap keyLongMap = testObj.keyLongMap(tablePrefix);
		keyLongMap.systemSetup();
		
		keyLongMap.putLong("testing", 5L);
		List<String> keys = new ArrayList<>();
		keys.add("testing");
		for (String key : keyLongMap.keySet()) {
			keys.remove(key);
		}
		assertTrue(keys.size() == 0);
	}
	
	@Test
	public void test_keySetKeyValueMap() {
		KeyValueMap keyValueMap = testObj.keyValueMap(tablePrefix);
		keyValueMap.systemSetup();
		
		keyValueMap.put("testing", "value");
		List<String> keys = new ArrayList<>();
		keys.add("testing");
		for (String key : keyValueMap.keySet()) {
			keys.remove(key);
		}
		assertTrue(keys.size() == 0);
	}
}
