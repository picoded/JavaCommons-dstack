package picoded.dstack.struct.simple;

// Test system include
import static org.junit.Assert.*;
import org.junit.*;

// Java includes
import java.util.*;

// External lib includes
import org.apache.commons.lang3.RandomUtils;

// Test depends
import picoded.core.conv.GUID;
import picoded.core.struct.CaseInsensitiveHashMap;
import picoded.core.struct.GenericConvertMap;
import picoded.dstack.*;
import picoded.dstack.struct.simple.*;

// DataObjectMap base test class
public class StructSimple_DataObjectMap_test {
	
	/// Test object
	public DataObjectMap mtObj = null;
	
	// To override for implementation
	//-----------------------------------------------------
	public DataObjectMap implementationConstructor() {
		return new StructSimple_DataObjectMap();
	}
	
	// Setup and sanity test
	//-----------------------------------------------------
	@Before
	public void setUp() {
		mtObj = implementationConstructor();
		mtObj.systemSetup();
	}
	
	@After
	public void tearDown() {
		if (mtObj != null) {
			mtObj.systemDestroy();
		}
		mtObj = null;
	}
	
	@Test
	public void constructorTest() {
		// not null check
		assertNotNull(mtObj);
		
		// Incremental maintenance
		mtObj.incrementalMaintenance();
		
		// run maintaince, no exception?
		mtObj.maintenance();
	}
	
	// Subset assertion
	//-----------------------------------------------
	
	/// Utility function, to ensure the expected values exists in map
	/// while allowing future test cases not to break when additional values
	/// like create timestamp is added.
	public void assetSubset(Map<String, Object> expected, Map<String, Object> result) {
		for (Map.Entry<String, Object> entry : expected.entrySet()) {
			assertEquals(entry.getValue(), result.get(entry.getKey()));
		}
	}
	
	// Test cases
	//-----------------------------------------------
	
	// Test utility used to generate random maps
	protected HashMap<String, Object> randomObjMap() {
		HashMap<String, Object> objMap = new CaseInsensitiveHashMap<String, Object>();
		objMap.put(GUID.base58(), RandomUtils.nextInt(0, (Integer.MAX_VALUE - 3)));
		objMap.put(GUID.base58(), -(RandomUtils.nextInt(0, (Integer.MAX_VALUE - 3))));
		objMap.put(GUID.base58(), GUID.base58());
		objMap.put(GUID.base58(), GUID.base58());
		
		objMap.put("num", RandomUtils.nextInt(0, (Integer.MAX_VALUE - 3)));
		objMap.put("str_val", GUID.base58());
		
		return objMap;
	}
	
	// @Test
	// public void invalidSetup() { //Numeric as table prefix tend to cuase
	// problems
	// DataObjectMap m;
	//
	// try {
	// m = new DataObjectMap(JStackObj, "1" + TestConfig.randomTablePrefix());
	// fail(); // if we got here, no exception was thrown, which is bad
	// } catch (Exception e) {
	// final String expected = "Invalid table name (cannot start with numbers)";
	// assertTrue("Missing Exception - " + expected,
	// e.getMessage().indexOf(expected) >= 0);
	// }
	// }
	
	@Test
	public void newEntryTest() {
		DataObject mObj = null;
		
		assertNotNull(mObj = mtObj.newEntry());
		mObj.put("be", "happy");
		mObj.saveDelta();
		
		String guid = null;
		assertNotNull(guid = mObj._oid());
		
		assertNotNull(mObj = mtObj.get(guid));
		assertEquals("happy", mObj.get("be"));
	}
	
	@Test
	public void basicTest() {
		String guid = GUID.base58();
		
		// Sanity check
		assertNull(mtObj.get(guid));
		
		// Random object to put in
		HashMap<String, Object> objMap = randomObjMap();
		DataObject mObj = null;
		
		// Puts in a new object, and get guid
		assertNotNull(guid = mtObj.newEntry(objMap)._oid());
		
		// Get and check with guid
		objMap.put("_oid", guid);
		assetSubset(objMap, (Map<String, Object>) mtObj.get(guid));
		
		objMap = randomObjMap();
		assertNotNull(guid = mtObj.newEntry(objMap)._oid());
		objMap.put("_oid", guid);
		assetSubset(objMap, mtObj.get(guid));
	}
	
	/// Checks if a blank object gets saved
	@Test
	public void blankObjectSave() {
		String guid = null;
		DataObject p = null;
		assertFalse(mtObj.containsKey("hello"));
		assertNotNull(p = mtObj.newEntry());
		assertNotNull(guid = p._oid());
		p.saveDelta();
		
		assertTrue(mtObj.containsKey(guid));
	}
	
	HashMap<String, Object> genNumStrObj(int number, String str) {
		HashMap<String, Object> objMap = new CaseInsensitiveHashMap<String, Object>();
		objMap.put("num", new Integer(number));
		objMap.put("str_val", str);
		return objMap;
	}
	
	HashMap<String, Object> genNumStrObj(int number, String str, int orderCol) {
		HashMap<String, Object> objMap = new CaseInsensitiveHashMap<String, Object>();
		objMap.put("num", new Integer(number));
		objMap.put("order", new Integer(orderCol));
		objMap.put("str_val", str);
		return objMap;
	}
	
	@Test
	public void indexBasedTestSetup() {
		mtObj.newEntry(genNumStrObj(1, "this"));
		mtObj.newEntry(genNumStrObj(2, "is"));
		mtObj.newEntry(genNumStrObj(3, "hello"));
		mtObj.newEntry(genNumStrObj(4, "world"));
		mtObj.newEntry(genNumStrObj(5, "program"));
		mtObj.newEntry(genNumStrObj(6, "in"));
		mtObj.newEntry(genNumStrObj(7, "this"));
	}
	
	/// Numeric based query test
	@Test
	public void indexBasedTest_num() {
		indexBasedTestSetup();
		
		DataObject[] qRes = null;
		assertNotNull(qRes = mtObj.query(null, null));
		assertEquals(7, qRes.length);
		assertEquals(7, mtObj.queryCount(null, null));
		
		assertNotNull(qRes = mtObj.query("num > ? AND num < ?", new Object[] { 2, 5 }, "num ASC"));
		assertEquals(2, qRes.length);
		assertEquals("hello", qRes[0].get("str_val"));
		assertEquals("world", qRes[1].get("str_val"));
		assertEquals(2, mtObj.queryCount("num > ? AND num < ?", new Object[] { 2, 5 }));
		
		assertNotNull(qRes = mtObj.query("num > ?", new Object[] { 2 }, "num ASC", 2, 2));
		assertEquals(2, qRes.length);
		assertEquals("program", qRes[0].get("str_val"));
		assertEquals("in", qRes[1].get("str_val"));
		
		assertEquals(5, mtObj.queryCount("num > ?", new Object[] { 2 }));
	}
	
	/// String based query test
	@Test
	public void indexBasedTest_string() {
		indexBasedTestSetup();
		
		DataObject[] qRes = null;
		assertNotNull(qRes = mtObj.query("str_val = ?", new Object[] { "this" }));
		assertEquals(2, qRes.length);
		assertEquals(2, mtObj.queryCount("str_val = ?", new Object[] { "this" }));
	}
	
	///
	/// An exception occurs, if a query fetch occurs with an empty table
	///
	@Test
	public void issue47_exceptionWhenTableIsEmpty() {
		DataObject[] qRes = null;
		assertNotNull(qRes = mtObj.query(null, null));
		assertEquals(0, qRes.length);
	}
	
	///
	/// Bad view index due to inner join instead of left join. Testing.
	///
	/// AKA: Incomplete object does not appear in view index
	///
	@Test
	public void innerJoinFlaw() {
		mtObj.newEntry(genNumStrObj(1, "hello world"));
		
		HashMap<String, Object> objMap = new CaseInsensitiveHashMap<String, Object>();
		objMap.put("num", new Integer(2));
		mtObj.newEntry(objMap).saveDelta();
		
		objMap = new CaseInsensitiveHashMap<String, Object>();
		objMap.put("str_val", "nope");
		mtObj.newEntry(objMap);
		
		DataObject[] qRes = null;
		assertNotNull(qRes = mtObj.query(null, null));
		assertEquals(3, qRes.length);
		
		assertNotNull(qRes = mtObj.query("num = ?", new Object[] { 1 }));
		assertEquals(1, qRes.length);
		
		assertNotNull(qRes = mtObj.query("num <= ?", new Object[] { 2 }));
		assertEquals(2, qRes.length);
		
		assertNotNull(qRes = mtObj.query("str_val = ?", new Object[] { "nope" }));
		assertEquals(1, qRes.length);
		
	}
	
	///
	/// Handle right outer closign bracket in DataObjectMap meta names
	///
	@Test
	public void mssqlOuterBrackerInMetaNameFlaw() {
		HashMap<String, Object> objMap = null;
		DataObject[] qRes = null;
		
		//
		// Setup vars to test against
		//
		objMap = new CaseInsensitiveHashMap<String, Object>();
		objMap.put("num[0].val", new Integer(2));
		mtObj.newEntry(objMap).saveDelta();
		
		objMap = new CaseInsensitiveHashMap<String, Object>();
		objMap.put("str[0].val", "nope");
		objMap.put("str[1].val", "rawr");
		mtObj.newEntry(objMap);
		
		objMap = new CaseInsensitiveHashMap<String, Object>();
		objMap.put("num[0].val", new Integer(2));
		objMap.put("str[0].val", "nope");
		mtObj.newEntry(objMap);
		
		//
		// Query to run
		//
		assertNotNull(qRes = mtObj.query("num[0].val = ?", new Object[] { 2 }));
		assertEquals(2, qRes.length);
		
		assertNotNull(qRes = mtObj.query("str[0].val = ?", new Object[] { "nope" }));
		assertEquals(2, qRes.length);
		
		assertNotNull(qRes = mtObj.query("str[1].val = ?", new Object[] { "rawr" }));
		assertEquals(1, qRes.length);
		
	}
	
	@Test
	public void missingStrError() {
		HashMap<String, Object> objMap = new HashMap<String, Object>();
		objMap.put("num", 123);
		
		String guid = GUID.base58();
		assertNull(mtObj.get(guid));
		assertNotNull(guid = mtObj.newEntry(objMap)._oid());
		
		DataObject[] qRes = null;
		assertNotNull(qRes = mtObj.query(null, null));
		assertEquals(1, qRes.length);
		
		objMap.put("_oid", guid);
		assetSubset(objMap, mtObj.get(guid));
	}
	
	@Test
	public void missingNumWithSomeoneElse() {
		mtObj.newEntry(genNumStrObj(1, "hello world"));
		
		HashMap<String, Object> objMap = new HashMap<String, Object>();
		objMap.put("str_val", "^_^");
		
		String guid = GUID.base58();
		assertNull(mtObj.get(guid));
		assertNotNull(guid = mtObj.newEntry(objMap)._oid());
		
		DataObject[] qRes = null;
		assertNotNull(qRes = mtObj.query(null, null));
		assertEquals(2, qRes.length);
		
		assertTrue(guid.equals(qRes[0]._oid()) || guid.equals(qRes[1]._oid()));
		
		objMap.put("_oid", guid);
		assetSubset(objMap, mtObj.get(guid));
	}
	
	@Test
	public void getFromKeyName_basic() {
		
		mtObj.newEntry(genNumStrObj(1, "one"));
		mtObj.newEntry(genNumStrObj(2, "two"));
		
		DataObject[] list = null;
		assertNotNull(list = mtObj.getFromKeyName("num"));
		assertEquals(2, list.length);
		
		String str = null;
		assertNotNull(str = list[0].getString("str_val"));
		assertTrue(str.equals("one") || str.equals("two"));
		
		assertNotNull(str = list[1].getString("str_val"));
		assertTrue(str.equals("one") || str.equals("two"));
		
	}
	
	@Test
	public void nonIndexedKeySaveCheck() {
		
		// Generates single node
		mtObj.newEntry(genNumStrObj(1, "hello world"));
		DataObject[] list = null;
		DataObject node = null;
		
		// Fetch that single node
		assertNotNull(list = mtObj.getFromKeyName("num"));
		assertEquals(1, list.length);
		assertNotNull(node = list[0]);
		
		// Put non indexed key in node, and save
		node.put("NotIndexedKey", "123");
		node.saveDelta();
		
		// Get the value, to check
		assertEquals(123, mtObj.get(node._oid()).get("NotIndexedKey"));
		
		// Refetch node, and get data, and validate
		assertNotNull(list = mtObj.getFromKeyName("num"));
		assertEquals(1, list.length);
		assertNotNull(list[0]);
		assertEquals(node._oid(), list[0]._oid());
		assertEquals(123, node.get("NotIndexedKey"));
		assertEquals(123, list[0].get("NotIndexedKey"));
	}
	
	@Test
	public void getFromKeyName_customKeys() {
		
		// Generates single node
		mtObj.newEntry(genNumStrObj(1, "hello world"));
		DataObject[] list = null;
		DataObject node = null;
		
		// Fetch that single node
		assertNotNull(list = mtObj.getFromKeyName("num"));
		assertEquals(1, list.length);
		assertNotNull(node = list[0]);
		
		// Put non indexed key in node, and save
		node.put("NotIndexedKey", "123");
		node.saveDelta();
		
		// Refetch node, and get data, and validate
		assertNotNull(list = mtObj.getFromKeyName("num"));
		assertEquals(1, list.length);
		assertNotNull(list[0]);
		assertEquals(node._oid(), list[0]._oid());
		assertEquals(123, node.get("NotIndexedKey"));
		assertEquals(123, list[0].get("NotIndexedKey"));
		
		// Fetch non indexed key
		assertNotNull(list = mtObj.getFromKeyName("NotIndexedKey"));
		assertEquals(1, list.length);
		
		// Assert equality
		assertEquals(node._oid(), list[0]._oid());
		
	}
	
	// Array values tests
	//-----------------------------------------------
	@Test
	public void jsonStorageTest() {
		Map<String, Object> data = new HashMap<String, Object>();
		data.put("name", "Hello");
		
		List<String> ohnoArray = Arrays.asList(new String[] { "oh", "no" });
		data.put("arrs", new ArrayList<String>(ohnoArray));
		
		DataObject mo = null;
		assertNotNull(mo = mtObj.newEntry(data));
		mo.saveDelta();
		
		DataObject to = null;
		assertNotNull(to = mtObj.get(mo._oid()));
		
		data.put("_oid", mo._oid());
		assetSubset(data, to);
		
		assertEquals(ohnoArray, to.get("arrs"));
	}
	
	@Test
	public void binaryStorageTest() {
		Map<String, Object> data = new HashMap<String, Object>();
		data.put("name", "Hello");
		data.put("bin", new byte[] { 1, 2, 3, 4, 5 });
		
		DataObject mo = null;
		assertNotNull(mo = mtObj.newEntry(data));
		mo.saveDelta();
		
		DataObject to = null;
		assertNotNull(to = mtObj.get(mo._oid()));
		
		assertTrue(data.get("bin") instanceof byte[]);
		assertTrue(to.get("bin") instanceof byte[]);
		
		assertArrayEquals((byte[]) (data.get("bin")), (byte[]) (to.get("bin")));
	}
	
	// Orderby sorting
	//-----------------------------------------------
	
	@Test
	public void T50_orderByTest() {
		
		// Lets just rescycle old test for the names
		mtObj.newEntry(genNumStrObj(1, "this", 5));
		mtObj.newEntry(genNumStrObj(2, "is", 4));
		mtObj.newEntry(genNumStrObj(3, "hello", 3));
		mtObj.newEntry(genNumStrObj(4, "world", 2));
		mtObj.newEntry(genNumStrObj(5, "program", 1));
		mtObj.newEntry(genNumStrObj(6, "in", 6));
		mtObj.newEntry(genNumStrObj(7, "this", 7));
		
		// Replicated a bug, where u CANNOT use orderby on a collumn your not
		// doing a where search
		DataObject[] qRes = mtObj.query("str_val = ?", new String[] { "this" }, "num ASC");
		assertEquals(qRes.length, 2);
		
		assertEquals("this", qRes[0].get("str_val"));
		assertEquals("this", qRes[1].get("str_val"));
		
		assertEquals(1, qRes[0].get("num"));
		assertEquals(7, qRes[1].get("num"));
		
		// Order by with offset
		qRes = mtObj.query(null, null, "num ASC", 2, 3);
		assertEquals(qRes.length, 3);
		
		assertEquals(3, qRes[0].get("num"));
		assertEquals(4, qRes[1].get("num"));
		assertEquals(5, qRes[2].get("num"));
		
		assertEquals("hello", qRes[0].get("str_val"));
		assertEquals("world", qRes[1].get("str_val"));
		assertEquals("program", qRes[2].get("str_val"));
		
		// Search
		qRes = mtObj.query("num >= ? AND num <= ?", new Object[] { 2, 6 });
		assertEquals(5, qRes.length);
		
		// Search with order by
		qRes = mtObj.query("num >= ? AND num <= ?", new Object[] { 2, 6 }, "order ASC");
		assertEquals(5, qRes.length);
		// To validate results
		
		// Search with order by with range
		qRes = mtObj.query("num >= ? AND num <= ?", new Object[] { 2, 6 }, "order ASC", 2, 2);
		assertEquals(2, qRes.length);
		
	}
	
	// @Test
	// public void orderByTestLoop() {
	// for(int i=0; i<25; ++i) {
	// orderByTest();
	//
	// tearDown();
	// setUp();
	// }
	// }
	
	// KeyName fetching test
	//-----------------------------------------------
	@Test
	public void getKeyNamesTest() {
		
		// Lets just rescycle old test for the names
		indexBasedTestSetup();
		
		Set<String> keyNames = mtObj.getKeyNames();
		Set<String> expected = new HashSet<String>(Arrays.asList(new String[] { "_oid", "num",
			"str_val" }));
		assertNotNull(keyNames);
		assertTrue(keyNames.containsAll(expected));
		
	}
	
	//// Mapping tests
	//// [FEATURE DROPPED]
	////-----------------------------------------------
	//
	// @Test
	// public void testSingleMappingSystem() {
	// 	mtObj.typeMap().clear();
	//
	// 	mtObj.putType("num", "INTEGER");
	// 	mtObj.putType("float", "FLOAT");
	// 	mtObj.putType("double", "double");
	// 	mtObj.putType("long", "long");
	//
	// 	assertEquals(mtObj.getType("num"), MetaType.INTEGER);
	// 	assertEquals(mtObj.getType("float"), MetaType.FLOAT);
	// 	assertEquals(mtObj.getType("double"), MetaType.DOUBLE);
	// 	assertEquals(mtObj.getType("long"), MetaType.LONG);
	// }
	//
	// @Test
	// public void testMapMappingSystem() {
	// 	mtObj.typeMap().clear();
	//
	// 	HashMap<String, Object> mapping = new HashMap<String, Object>();
	// 	mapping.put("num", "INTEGER");
	// 	mapping.put("float", "FLOAT");
	// 	mapping.put("double", "double");
	// 	mapping.put("long", "long");
	// 	mapping.put("mixed", "MIXED");
	// 	mapping.put("uuid-array", "UUID_ARRAY");
	//
	// 	mtObj.setMappingType(mapping);
	//
	// 	assertEquals(mtObj.getType("num"), MetaType.INTEGER);
	// 	assertEquals(mtObj.getType("float"), MetaType.FLOAT);
	// 	assertEquals(mtObj.getType("double"), MetaType.DOUBLE);
	// 	assertEquals(mtObj.getType("long"), MetaType.LONG);
	// 	assertEquals(mtObj.getType("mixed"), MetaType.MIXED);
	// 	assertEquals(mtObj.getType("uuid-array"), MetaType.UUID_ARRAY);
	// }
	
	//// Demo code : kept here for reference
	////-----------------------------------------------
	// @Test
	// public void demoCode() {
	// 	// Initiate a meta table
	// 	DataObjectMap table = (new JStruct()).getDataObjectMap("demo");
	//
	// 	// Adding new object?
	// 	DataObject mObj = table.newEntry();
	// 	mObj.put("be", "happy");
	// 	mObj.put("num", new Integer(1));
	// 	mObj.saveDelta();
	//
	// 	// Doing a query
	// 	DataObject[] qRes = null;
	// 	assertNotNull(qRes = table.query("num > ? OR be = ?", new Object[] { 0, "happy" }));
	// 	// Each object has a base68 GUID
	// 	String guid = qRes[0]._oid();
	// 	assertNotNull(table.queryKeys("num > ? OR be = ?", new Object[] { 0, "happy" }, null, 0, 0));
	// 	assertNotNull(table.get(guid, false));
	// 	assertNotNull(table.getFromKeyName("happy", null));
	// 	Map<String, Object> objMap = new CaseInsensitiveHashMap<String, Object>();
	// 	objMap.put("hello", qRes);
	// 	assertNotNull(table.append(guid, qRes[0]));
	// 	assertNotNull(table.append("test", qRes[0]));
	// 	assertNotNull(table.getKeyNames(0));
	// 	assertNotNull(table.getKeyNames(-1));
	// 	assertNotNull(table.getFromKeyName_id("happy"));
	// }
	
	// remove meta object support
	//-----------------------------------------------
	@Test
	public void removeViaDataObject() {
		
		// Lets just rescycle old test for some dummy data
		basicTest();
		
		// Lets get DataObject list
		DataObject[] oRes = null;
		assertNotNull(oRes = mtObj.query(null, null));
		assertTrue(oRes.length > 0);
		
		// Lets remove one object
		mtObj.remove(oRes[0]);
		
		// Lets query to make sure its removed
		DataObject[] qRes = null;
		assertNotNull(qRes = mtObj.query(null, null));
		assertEquals(oRes.length - 1, qRes.length);
	}
	
	@Test
	public void removeViaMetaOID() {
		
		// Lets just rescycle old test for some dummy data
		basicTest();
		
		// Lets get DataObject list
		DataObject[] oRes = null;
		assertNotNull(oRes = mtObj.query(null, null));
		assertTrue(oRes.length > 0);
		
		// Lets remove one object
		mtObj.remove(oRes[0]._oid());
		
		// Lets query to make sure its removed
		DataObject[] qRes = null;
		assertNotNull(qRes = mtObj.query(null, null));
		assertEquals(oRes.length - 1, qRes.length);
	}
	
	@Test
	public void queryWithOID() {
		
		// Lets just rescycle old test for some dummy data
		basicTest();
		
		// Lets get DataObject list
		DataObject[] oRes = null;
		assertNotNull(oRes = mtObj.query(null, null));
		assertTrue(oRes.length > 0);
		
		// Lets remove one object
		DataObject ref = oRes[0];
		
		// Lets query to make sure it works
		DataObject[] qRes = null;
		assertNotNull(qRes = mtObj.query("_oid = ?", new Object[] { ref._oid() }));
		assertEquals(1, qRes.length);
		assertEquals(ref._oid(), qRes[0]._oid());
		
		// Lets query to make sure it works
		qRes = null;
		assertNotNull(qRes = mtObj.query("num != ? AND _oid = ?", new Object[] { -1, ref._oid() }));
		assertEquals(1, qRes.length);
		assertEquals(ref._oid(), qRes[0]._oid());
	}
	
	// Query unset parameters
	//-----------------------------------------------
	
	public void testWithUnserParameters_setup() {
		// Meta Object to manipulate around with
		DataObject mObj = null;
		
		// Lets set the result that should be there
		assertNotNull(mObj = mtObj.newEntry());
		mObj.put("dun", "worry");
		mObj.put("be", "happy");
		mObj.saveDelta();
		
		// lets setup a partial
		assertNotNull(mObj = mtObj.newEntry());
		// mObj.put("dun", "have");
		mObj.put("be", "missing");
		mObj.saveDelta();
		
		// Lets set the result that should be there
		assertNotNull(mObj = mtObj.newEntry());
		mObj.put("dun", "be");
		mObj.put("be", "ok");
		mObj.saveDelta();
	}
	
	@Test
	public void testWithUnsetParameters_queryNull() {
		// Setup 
		this.testWithUnserParameters_setup();
		
		// Meta Object to manipulate around with
		DataObject[] queryRes = null;
		
		// Query for objects, with exsiting properties
		queryRes = mtObj.query("what = ?", new Object[] { null });
		assertEquals(3, queryRes.length);
		
		// Query for objects, with missing property
		queryRes = mtObj.query("dun != ?", new Object[] { null });
		assertEquals(2, queryRes.length);
	}
	
	@Test
	public void testWithUnsetParameters_query() {
		// Setup 
		this.testWithUnserParameters_setup();
		
		// Meta Object to manipulate around with
		DataObject[] queryRes = null;
		
		// Query for objects, with exsiting properties
		queryRes = mtObj.query("be != ?", new Object[] { "evil" });
		assertEquals(3, queryRes.length);
		
		// Query for objects, with missing property
		queryRes = mtObj.query("dun != ?", new Object[] { "evil" });
		assertEquals(3, queryRes.length);
	}
	
	@Test
	public void testWithUnsetParameters_orderBy() {
		// Setup 
		this.testWithUnserParameters_setup();
		
		// Meta Object to manipulate around with
		DataObject[] queryRes = null;
		
		// Query for objects, with exsiting properties
		queryRes = mtObj.query("be != ?", new Object[] { "evil" }, "be ASC");
		assertEquals(3, queryRes.length);
		
		// Query for objects, with missing property
		queryRes = mtObj.query("be != ?", new Object[] { "evil" }, "dun ASC");
		assertEquals(3, queryRes.length);
		
	}
	
	// Nested data test
	//-----------------------------------------------
	
	@Test
	public void nestedDataTest() {
		//
		// Initial object setup
		//
		
		// Lets setup the test object
		DataObject testObj = mtObj.newEntry();
		testObj.put("hello", "world");
		testObj.saveDelta();
		String _id = testObj.getString("_oid");
		
		// Lets get the object back
		testObj = mtObj.get(_id);
		assertNotNull(testObj);
		
		// Lets get the inner map
		GenericConvertMap<String, Object> innerMap = testObj.getGenericConvertStringMap("innerMap",
			"{ 'the':1 }");
		assertNotNull(innerMap);
		
		// Setup sublist
		List<String> subList = new ArrayList<String>();
		subList.add("only");
		innerMap.put("and", subList);
		
		// Save the innermap
		testObj.put("innerMap", innerMap);
		testObj.saveDelta();
		
		//
		// Get, and modify
		//
		
		// Lets get the object again
		testObj = null;
		testObj = mtObj.get(_id);
		assertNotNull(testObj);
		
		// Lets get the innerMap again
		innerMap = testObj.getGenericConvertStringMap("innerMap", "{}");
		assertEquals(1, innerMap.getInt("the"));
		
		// Lets get the nested list
		subList = innerMap.getList("and", null);
		assertNotNull(subList);
		
		// Lets get the nested list string
		assertEquals("only", subList.get(0));
		
		// Lets modify it
		subList.add("no more");
		innerMap.put("and", subList);
		testObj.put("innerMap", innerMap);
		testObj.saveDelta();
		
		//
		// Get again
		//
		
		// Lets get the object again
		testObj = null;
		testObj = mtObj.get(_id);
		assertNotNull(testObj);
		
		// Lets get the innerMap again
		innerMap = testObj.getGenericConvertStringMap("innerMap", "{}");
		assertEquals(1, innerMap.getInt("the"));
		
		// Lets get the nested list
		subList = innerMap.getList("and", null);
		assertNotNull(subList);
		
		// and assert
		assertEquals("only", subList.get(0));
		assertEquals("no more", subList.get(1));
		
	}
	
	// Random object, and iteration support
	//-----------------------------------------------
	
	@Test
	public void randomObjectTest() {
		assertNull(mtObj.randomObject());
		assertNull(mtObj.looselyIterateObject(null));
		basicTest();
		assertNotNull(mtObj.randomObject());
		assertNotNull(mtObj.looselyIterateObject(null));
	}
}
