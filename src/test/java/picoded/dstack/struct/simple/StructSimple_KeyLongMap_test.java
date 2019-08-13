package picoded.dstack.struct.simple;

// Target test class
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.security.SecureRandom;
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
	public void getUnknownKey() throws Exception {
		assertNull(null, testObj.get("nullKey"));
	}
	
	@Test
	public void setNullValueToKey() throws Exception {
		testObj.putValue("nullKey", null);
		
		assertNull(testObj.get("nullKey"));
		assertNull(testObj.getValue("nullKey"));
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
		
		// Search for null
		assertEquals(
			new HashSet<String>(Arrays.asList(new String[] { "yes", "hello", "this", "is" })),
			testObj.keySet(null));
	}
	
	@Test
	public void weakCompareAndSetTest() throws Exception {
		testObj.put("first", 3L);
		boolean result = testObj.weakCompareAndSet("first", 3L, 4L);
		assertTrue(result);
		assertEquals(4L, testObj.getValue("first").longValue());
		
		result = testObj.weakCompareAndSet("first", 3L, 5L);
		assertFalse(result);
		
		result = testObj.weakCompareAndSet("unknown value", null, 3L);
		assertTrue(result);
		assertEquals(3L, testObj.getValue("unknown value").longValue());
		
		result = testObj.weakCompareAndSet("unknown value", 3L, 5L);
		assertTrue(result);
		assertEquals(5L, testObj.getValue("unknown value").longValue());
	}
	
	@Test
	public void incrementAndGet() throws Exception {
		testObj.put("incrementAndGet", 4L);
		long value = testObj.incrementAndGet("incrementAndGet");
		assertEquals(5L, value);
		assertEquals(5L, testObj.getValue("incrementAndGet").longValue());
	}
	
	@Test
	public void getAndIncrement() throws Exception {
		testObj.put("getAndIncrement", 4L);
		long value = testObj.getAndIncrement("getAndIncrement");
		assertEquals(4L, value);
		assertEquals(5L, testObj.getValue("getAndIncrement").longValue());
	}
	
	@Test
	public void decrementAndGet() throws Exception {
		testObj.put("decrementAndGet", 4L);
		long value = testObj.decrementAndGet("decrementAndGet");
		assertEquals(3L, value);
		assertEquals(3L, testObj.getValue("decrementAndGet").longValue());
	}
	
	@Test
	public void getAndDecrement() throws Exception {
		testObj.put("getAndDecrement", 4L);
		long value = testObj.getAndDecrement("getAndDecrement");
		assertEquals(4L, value);
		assertEquals(3L, testObj.getValue("getAndDecrement").longValue());
	}
	
	@Test
	public void maintenanceCheckTest() throws Exception {
		// put a key with lifespan of 100ms
		testObj.put("shortLife", 23L);
		testObj.get("shortLife").setLifeSpan(100);
		
		// Ensure the life is over
		Thread.sleep(100 + expireAccuracy());
		
		testObj.maintenance();
		
		assertNull(testObj.get("shortLife"));
	}
	
	//-----------------------------------------------------------
	//
	//  Number accuracy issues
	//
	//-----------------------------------------------------------
	
	public void testWeakCompareAndSetAccuracy(long testValue) {
		// Setup the params
		String lockID = "file~lock~test";
		long lockTimeout = 10000;

		// Lets try to do a lock
		assertTrue( testObj.weakCompareAndSet(lockID, 0l, testValue) );
		testObj.setLifeSpan(lockID, lockTimeout);

		// Validate the existing value, this guard against a narrow
		// lock expriy window which occurs between
		// a weakCompareAndSet, and the setLifeSpan command.
		long registeredToken = testObj.getLong(lockID);
		assertEquals(testValue, registeredToken);
		
		// Current lifespan fetching
		long currentLifespan = testObj.getLifespan(lockID);
		assertNotEquals(0l, currentLifespan);
		assertTrue(currentLifespan > 0);

		// Lets delete the value (we are done)
		testObj.remove(lockID);
	}

	// NOTE : Large long values (such as randomLong)
	//        is currently a known issue for mysql
	//        as it seems to clamp its accuracy to 20 digits (somehow)
	//        when its currently speced for 24 digits
	@Test
	public void weakCompareAndSet_randomLong() {
		// Lets derive the "new" lock token - randomly!
		long testValue = Math.abs((new SecureRandom()).nextLong());
		testWeakCompareAndSetAccuracy(testValue);
	}

	@Test
	public void weakCompareAndSet_randomInt() {
		// Lets derive the "new" lock token - randomly!
		long testValue = Math.abs((new SecureRandom()).nextInt());
		testWeakCompareAndSetAccuracy(testValue);
	}

	@Test
	public void weakCompareAndSet_randomInt_multipleTimes() {
		for(int i=0; i<10; ++i) {
			weakCompareAndSet_randomInt();
		}
	}

	@Test
	public void weakCompareAndSet_maxInt() {
		// Max INT
		long testValue = Integer.MAX_VALUE;
		testWeakCompareAndSetAccuracy(testValue);
	}

	@Test
	public void weakCompareAndSet_maxIntPlus() {
		// Max INT +++
		long testValue = ((long)Integer.MAX_VALUE)+1234l;
		testWeakCompareAndSetAccuracy(testValue);
	}

	//-----------------------------------------------------------
	//
	//  TTL accuracy dependent tests
	//
	//-----------------------------------------------------------
	
	// Putting object that exist in the past is currently undefined behaviour
	// @Test
	// public void getValueFromExpiredKey() throws Exception {
	// 	long expiredTime = System.currentTimeMillis() - 5000;
	// 	testObj.putWithExpiry("expiredKey", 12L, expiredTime);
	
	// 	assertNull(testObj.getValue("expiredKey"));
	// 	assertNull(testObj.get("expiredKey"));
	// }
	
	// Exprie timestamp accuracy overwrites
	public long expireAccuracy() {
		return 2000;
	}
	
	@Test
	public void getExpireTime() throws Exception {
		long expireTime = System.currentTimeMillis() * 2;
		testObj.putWithExpiry("yes", 0L, expireTime);
		
		assertNotNull(testObj.getExpiry("yes"));
		
		long storedExpireTime = testObj.getExpiry("yes");
		assertTrue(expireTime > (storedExpireTime - expireAccuracy()));
		assertTrue(expireTime < (storedExpireTime + expireAccuracy()));
	}
	
	@Test
	public void setExpireTime() throws Exception {
		long expireTime = System.currentTimeMillis() + 60000;
		testObj.putWithExpiry("yes", 0L, expireTime);
		
		long newExpireTime = testObj.getExpiry("yes") * 2;
		testObj.setExpiry("yes", newExpireTime);
		
		long fetchedExpireTime = testObj.getExpiry("yes");
		assertNotNull(fetchedExpireTime);
		assertTrue(newExpireTime > (fetchedExpireTime - expireAccuracy()));
		assertTrue(newExpireTime < (fetchedExpireTime + expireAccuracy()));
		
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
