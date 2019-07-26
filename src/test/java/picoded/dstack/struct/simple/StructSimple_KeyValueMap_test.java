package picoded.dstack.struct.simple;

// Target test class
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;

// Test Case include
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// Test depends
import picoded.dstack.*;
import picoded.dstack.struct.simple.*;

public class StructSimple_KeyValueMap_test {
	
	// Test object for reuse
	public KeyValueMap testObj = null;
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Note that this implementation constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public KeyValueMap implementationConstructor() {
		return new StructSimple_KeyValueMap();
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
	
	// basic test
	//-----------------------------------------------------
	
	@Test
	public void simpleHasPutHasGet() throws Exception {
		assertFalse(testObj.containsKey("hello"));
		testObj.put("hello", "world");
		assertEquals("world", testObj.getValue("hello"));
		assertTrue(testObj.containsKey("hello"));
		assertEquals("world", testObj.get("hello").toString());
	}
	
	@Test
	public void getExpireTime() throws Exception {
		long expireTime = System.currentTimeMillis() + 1001000L;
		testObj.putWithExpiry("yes", "no", expireTime);
		assertNotNull(testObj.getExpiry("yes"));
		
		// Our testing requirements is for an acuracy range of 2 seconds
		assertTrue((expireTime - 2000L) <= testObj.getExpiry("yes"));
		assertTrue((expireTime + 2000L) >= testObj.getExpiry("yes"));
	}
	
	@Test
	public void setExpireTime() throws Exception {
		long expireTime = System.currentTimeMillis() + 1001000L;
		testObj.putWithExpiry("yes", "no", expireTime);
		
		long newExpireTime = testObj.getExpiry("yes") + 5001000L;
		testObj.setExpiry("yes", newExpireTime);
		
		long fetchedExpireTime = testObj.getExpiry("yes");
		assertNotNull(fetchedExpireTime);
		
		// Our testing requirements is for an acuracy range of 2 seconds
		assertTrue(newExpireTime - 2000L <= fetchedExpireTime);
		assertTrue(newExpireTime + 2000L >= fetchedExpireTime);
	}
	
	@Test
	public void setLifeSpan() throws Exception {
		long lifespanTime = 4 * 24 * 60 * 60 * 60 * 1000L;
		testObj.putWithLifespan("yes", "no", lifespanTime);
		
		long newLifespanTime = testObj.getExpiry("yes");
		testObj.setLifeSpan("yes", newLifespanTime);
		
		assertNotNull(testObj.getLifespan("yes"));
	}
	
	@Test
	public void keySetTest() throws Exception {
		assertEquals(new HashSet<String>(), testObj.keySet());
		assertEquals(new HashSet<String>(), testObj.keySet("world"));
		
		testObj.put("yes", "no");
		testObj.put("hello", "world");
		testObj.put("this", "world");
		testObj.put("is", "sparta");
		
		assertEquals("no", testObj.getValue("yes"));
		assertEquals("world", testObj.getString("hello"));
		assertEquals("world", testObj.getString("this"));
		assertEquals("sparta", testObj.getString("is"));
		
		assertEquals(new HashSet<String>(Arrays.asList(new String[] { "hello", "this" })),
			testObj.keySet("world"));
	}
	
	@Test
	public void SLOW_testColumnExpiration() throws Exception {
		// set column expiration time to current time + 1 secs.
		long expirationTime = System.currentTimeMillis() + 1 * 1000;
		testObj.putWithExpiry("yes", "no", expirationTime);
		
		// before the expiration time key will not be null.
		assertNotNull(testObj.get("yes"));
		
		// sleep the execution for 1.5 secs so the inserted key gets expired.
		Thread.sleep(1500);
		
		// key should be null after expiration time.
		assertEquals(null, testObj.get("yes"));
	}
	
	// CRUD operations
	//-----------------------------------------------------
	
	@Test
	public void removeObject() throws Exception {
		testObj.put("removeMe", "no");
		testObj.put("pleaseRemove", "okay");
		
		assertEquals(testObj.getValue("pleaseRemove"), "okay");
		
		testObj.remove("pleaseRemove");
		
		assertNull(testObj.getValue("pleaseRemove"));
	}
	
}
