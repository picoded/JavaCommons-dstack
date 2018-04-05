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
import picoded.dstack.KeyLongMap;

public class StructSimple_KeyLongMap_test {
	// Test object for reuse
	public KeyLongMap testObj = null;

	// To override for implementation
	//-----------------------------------------------------

	/// Note that this implementation constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public KeyLongMap implementationConstructor() {
		return new StructSimple_KeyLongMap();
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
		testObj.put("hello", 2L);
		assertEquals(2L, testObj.getValue("hello").longValue());
		assertTrue(testObj.containsKey("hello"));
		assertEquals("2", testObj.get("hello").toString());
	}

	@Test
	public void getExpireTime() throws Exception {
		long expireTime = System.currentTimeMillis() * 2;
		testObj.putWithExpiry("yes", 0L, expireTime);

		assertNotNull(testObj.getExpiry("yes"));
		assertEquals(expireTime, testObj.getExpiry("yes"));
	}

	@Test
	public void setExpireTime() throws Exception {
		long expireTime = System.currentTimeMillis() * 2;
		testObj.putWithExpiry("yes", 0L, expireTime);

		long newExpireTime = testObj.getExpiry("yes") * 2;
		testObj.setExpiry("yes", newExpireTime);

		long fetchedExpireTime = testObj.getExpiry("yes");
		assertNotNull(fetchedExpireTime);
		assertEquals(fetchedExpireTime, newExpireTime);
	}

	@Test
	public void setLifeSpan() throws Exception {
		long lifespanTime = 4 * 24 * 60 * 60 * 60 * 1000;
		testObj.putWithLifespan("yes", 0L, lifespanTime);

		long newLifespanTime = testObj.getExpiry("yes");
		testObj.setLifeSpan("yes", newLifespanTime);

		assertNotNull(testObj.getLifespan("yes"));
	}

	@Test
	public void keySetTest() throws Exception {
		assertEquals(new HashSet<String>(), testObj.keySet());
		assertEquals(new HashSet<String>(), testObj.keySet(2L));

		testObj.put("yes", 1L);
		testObj.put("hello", 1L);
		testObj.put("this", 300L);
		testObj.put("is", 4000L);

		assertEquals(1L, testObj.getValue("yes").longValue());
		assertEquals("1", testObj.getString("hello"));
		assertEquals(300L, testObj.getValue("this").longValue());
		assertEquals(4000L, testObj.getValue("is").longValue());

		assertEquals(new HashSet<String>(Arrays.asList(new String[] { "yes", "hello" })),
				testObj.keySet(1L));
	}

	@Test
	public void SLOW_testColumnExpiration() throws Exception {
		// set column expiration time to current time + 1 secs.
		long expirationTime = System.currentTimeMillis() + 1 * 1000;
		testObj.putWithExpiry("yes", 0L, expirationTime);

		// before the expiration time key will not be null.
		assertNotNull(testObj.get("yes"));

		// sleep the execution for 1.5 secs so the inserted key gets expired.
		Thread.sleep(1500);

		// key should be null after expiration time.
		assertEquals(null, testObj.get("yes"));
	}

}

