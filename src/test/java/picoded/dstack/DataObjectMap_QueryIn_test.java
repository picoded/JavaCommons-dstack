package picoded.dstack;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import picoded.dstack.struct.simple.StructSimple_DataObjectMap;

/**
 * Unit tests verifying the contract and edge cases of DataObjectMap.queryIn
 * and DataObjectMap.queryIn_id implementations.
 *
 * Validates input validation (null/empty property names, null/empty collections),
 * size limit boundaries (MAX_QUERY_IN_SIZE = 100), compound WHERE clauses,
 * and result ordering and pagination.
 */
public class DataObjectMap_QueryIn_test {

	private DataObjectMap dataMap = null;

	/**
	 * Initializes a fresh in-memory StructSimple_DataObjectMap before each test.
	 */
	@Before
	public void setUp() {
		dataMap = new StructSimple_DataObjectMap();
		dataMap.systemSetup();
	}

	/**
	 * Destroys the in-memory map and releases test resources after each test.
	 */
	@After
	public void tearDown() {
		if (dataMap != null) {
			dataMap.systemDestroy();
		}
		dataMap = null;
	}

	/**
	 * Tests that passing null or an empty Collection to queryIn and queryIn_id
	 * safely returns an empty array rather than throwing NullPointerException.
	 */
	@Test
	public void testNullOrEmptyCollectionReturnsEmptyArray() {
		DataObject[] objsNull = dataMap.queryIn("status", null);
		assertNotNull(objsNull);
		assertEquals(0, objsNull.length);

		String[] idsNull = dataMap.queryIn_id("status", null);
		assertNotNull(idsNull);
		assertEquals(0, idsNull.length);

		DataObject[] objsEmpty = dataMap.queryIn("status", Collections.emptyList());
		assertNotNull(objsEmpty);
		assertEquals(0, objsEmpty.length);

		String[] idsEmpty = dataMap.queryIn_id("status", Collections.emptyList());
		assertNotNull(idsEmpty);
		assertEquals(0, idsEmpty.length);
	}

	/**
	 * Tests that passing a null propertyName to queryIn throws IllegalArgumentException.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testNullPropertyNameThrowsExceptionForQueryIn() {
		dataMap.queryIn(null, Arrays.asList("ACTIVE"));
	}

	/**
	 * Tests that passing an empty or whitespace-only propertyName to queryIn throws IllegalArgumentException.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testEmptyPropertyNameThrowsExceptionForQueryIn() {
		dataMap.queryIn("   ", Arrays.asList("ACTIVE"));
	}

	/**
	 * Tests that passing a null propertyName to queryIn_id throws IllegalArgumentException.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testNullPropertyNameThrowsExceptionForQueryInId() {
		dataMap.queryIn_id(null, Arrays.asList("ACTIVE"));
	}

	/**
	 * Tests that passing an empty string propertyName to queryIn_id throws IllegalArgumentException.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testEmptyPropertyNameThrowsExceptionForQueryInId() {
		dataMap.queryIn_id("", Arrays.asList("ACTIVE"));
	}

	/**
	 * Tests that querying exactly MAX_QUERY_IN_SIZE (100) items succeeds without throwing an exception.
	 */
	@Test
	public void testBoundary100ItemsSucceeds() {
		List<String> values100 = new ArrayList<String>();
		for (int i = 0; i < 100; i++) {
			values100.add("item_" + i);
		}

		DataObject[] resultObjs = dataMap.queryIn("category", values100);
		assertNotNull(resultObjs);
		assertEquals(0, resultObjs.length);

		String[] resultIds = dataMap.queryIn_id("category", values100);
		assertNotNull(resultIds);
		assertEquals(0, resultIds.length);
	}

	/**
	 * Tests that querying 101 items (exceeding MAX_QUERY_IN_SIZE = 100)
	 * throws IllegalArgumentException for queryIn.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testBoundary101ItemsThrowsExceptionForQueryIn() {
		List<String> values101 = new ArrayList<String>();
		for (int i = 0; i <= 100; i++) {
			values101.add("item_" + i);
		}
		dataMap.queryIn("category", values101);
	}

	/**
	 * Tests that querying 101 items (exceeding MAX_QUERY_IN_SIZE = 100)
	 * throws IllegalArgumentException for queryIn_id.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testBoundary101ItemsThrowsExceptionForQueryInId() {
		List<String> values101 = new ArrayList<String>();
		for (int i = 0; i <= 100; i++) {
			values101.add("item_" + i);
		}
		dataMap.queryIn_id("category", values101);
	}

	/**
	 * Tests queryIn and queryIn_id with matching data entries.
	 * Verifies matching on specific field values, single value matches,
	 * and queries returning zero results for non-existent values.
	 */
	@Test
	public void testQueryInWithMatchingData() {
		DataObject obj1 = dataMap.newEntry();
		obj1.put("billID", "BILL_1");
		obj1.put("status", "SUCCESS");
		obj1.saveAll();

		DataObject obj2 = dataMap.newEntry();
		obj2.put("billID", "BILL_2");
		obj2.put("status", "FAILED");
		obj2.saveAll();

		DataObject obj3 = dataMap.newEntry();
		obj3.put("billID", "BILL_3");
		obj3.put("status", "PENDING");
		obj3.saveAll();

		DataObject obj4 = dataMap.newEntry();
		obj4.put("billID", "BILL_4");
		obj4.put("status", "SUCCESS");
		obj4.saveAll();

		// Query for BILL_1 and BILL_3
		List<String> queryBills = Arrays.asList("BILL_1", "BILL_3");
		DataObject[] matchedObjs = dataMap.queryIn("billID", queryBills);
		assertEquals(2, matchedObjs.length);

		List<String> matchedBillIds = new ArrayList<String>();
		for (DataObject matched : matchedObjs) {
			matchedBillIds.add((String) matched.get("billID"));
		}
		assertTrue(matchedBillIds.contains("BILL_1"));
		assertTrue(matchedBillIds.contains("BILL_3"));

		// Query IDs for BILL_2
		String[] matchedIds = dataMap.queryIn_id("billID", Arrays.asList("BILL_2"));
		assertEquals(1, matchedIds.length);
		assertEquals(obj2._oid(), matchedIds[0]);

		// Query for non-existent value
		DataObject[] noMatch = dataMap.queryIn("billID", Arrays.asList("BILL_999"));
		assertEquals(0, noMatch.length);
	}

	/**
	 * Tests queryIn with orderByStr, offset, and limit.
	 * Verifies that pagination parameters correctly slice the matching result set.
	 */
	@Test
	public void testQueryInWithSortingAndPagination() {
		for (int i = 1; i <= 5; i++) {
			DataObject entry = dataMap.newEntry();
			entry.put("groupKey", "GRP_A");
			entry.put("sortOrder", Integer.valueOf(i));
			entry.saveAll();
		}

		// Offset 1, Limit 2
		DataObject[] paged = dataMap.queryIn("groupKey", Arrays.asList("GRP_A"), "sortOrder ASC", 1, 2);
		assertEquals(2, paged.length);
		assertEquals(Integer.valueOf(2), paged[0].get("sortOrder"));
		assertEquals(Integer.valueOf(3), paged[1].get("sortOrder"));
	}

	/**
	 * Tests queryIn and queryIn_id with an additional WHERE clause and arguments.
	 * Verifies compound boolean evaluation: property IN (...) AND (additionalWhereQuery).
	 */
	@Test
	public void testQueryInWithAdditionalWhereClause() {
		DataObject obj1 = dataMap.newEntry();
		obj1.put("billID", "BILL_1");
		obj1.put("status", "SUCCESS");
		obj1.saveAll();

		DataObject obj2 = dataMap.newEntry();
		obj2.put("billID", "BILL_1");
		obj2.put("status", "FAILED");
		obj2.saveAll();

		DataObject obj3 = dataMap.newEntry();
		obj3.put("billID", "BILL_2");
		obj3.put("status", "SUCCESS");
		obj3.saveAll();

		DataObject obj4 = dataMap.newEntry();
		obj4.put("billID", "BILL_2");
		obj4.put("status", "FAILED");
		obj4.saveAll();

		// Query IN ("BILL_1", "BILL_2") AND status = ? ("SUCCESS")
		DataObject[] matched = dataMap.queryIn("billID", Arrays.asList("BILL_1", "BILL_2"), "status = ?", new Object[] { "SUCCESS" });
		assertEquals(2, matched.length);
		List<String> oids = new ArrayList<String>();
		for (DataObject d : matched) {
			oids.add(d._oid());
		}
		assertTrue(oids.contains(obj1._oid()));
		assertTrue(oids.contains(obj3._oid()));

		// Query IN_id ("BILL_1", "BILL_2") AND status = ? ("FAILED")
		String[] failedIds = dataMap.queryIn_id("billID", Arrays.asList("BILL_1", "BILL_2"), "status = ?", new Object[] { "FAILED" });
		assertEquals(2, failedIds.length);
		List<String> failedList = Arrays.asList(failedIds);
		assertTrue(failedList.contains(obj2._oid()));
		assertTrue(failedList.contains(obj4._oid()));
	}
}
