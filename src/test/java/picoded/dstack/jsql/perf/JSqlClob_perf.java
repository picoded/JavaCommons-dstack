// package picoded.dstack.jsql.perf;

// // Test system include
// import static org.junit.Assert.*;
// import org.junit.*;
// import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
// import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
// import com.carrotsearch.junitbenchmarks.BenchmarkRule;

// // Java includes
// import java.util.*;

// // External lib includes
// import org.apache.commons.lang3.RandomUtils;

// import picoded.core.conv.ConvertJSON;
// // Test depends
// import picoded.core.conv.GUID;
// import picoded.core.struct.CaseInsensitiveHashMap;
// import picoded.dstack.jsql.connector.*;
// import picoded.dstack.jsql.*;
// import picoded.dstack.jsql.JSqlTestConfig;

// /// Testing of DataTable full CLOB structure performance
// public class JSqlClob_perf extends AbstractBenchmark {

// 	/// Test object
// 	public JSql jsqlObj = null;

// 	/// Table name to test
// 	public String tablename = null;

// 	// To override for implementation
// 	//-----------------------------------------------------

// 	/// Note that this SQL connector constructor
// 	/// is to be overriden for the various backend
// 	/// specific test cases
// 	public JSql jsqlConnection() {
// 		return JSqlTestConnection.sqlite();
// 	}

// 	// JUnit setup / teardown call
// 	//-----------------------------------------------------
// 	@Before
// 	public void setUp() {
// 		tablename = "mtp_" + JSqlTestConfig.randomTablePrefix();

// 		jsqlObj = jsqlConnection();

// 		tableSetup();
// 		prepareTestObjects();
// 	}

// 	@After
// 	public void tearDown() {
// 		if (jsqlObj != null) {
// 			tableTeardown();
// 		}
// 		jsqlObj = null;
// 	}

// 	// Table setup and teardown
// 	//-----------------------------------------------------

// 	public void tableSetup() {
// 		jsqlObj.createTable(tablename, new String[] { //
// 			// Primary key, as classic int, this is used to lower SQL
// 			// fragmentation level, and index memory usage. And is not accessible.
// 			// Sharding and uniqueness of system is still maintained by GUID's
// 				"pKy", //
// 				"oID", //
// 				"jsonData" // 
// 			}, new String[] { //
// 			"BIGINT PRIMARY KEY AUTOINCREMENT", "VARCHAR(64)", //
// 				"TEXT" // 
// 			});

// 		// This optimizes query by object keys
// 		// + oID
// 		jsqlObj.createIndex( //
// 			tablename, "oID", "UNIQUE", "unq" //
// 		); //
// 	}

// 	public void tableTeardown() {
// 		jsqlObj.dropTable(tablename);
// 	}

// 	// Performance test objects setup
// 	//-----------------------------------------------------

// 	/// Small map of 10 string, and 10 numeric properties
// 	Map<String, Object> smallMap = null;

// 	/// Medium map of 200 string, and 250 numeric properties
// 	Map<String, Object> mediumMap = null;

// 	/// Large map of 1000 string, and 1000 numeric properties
// 	Map<String, Object> largeMap = null;

// 	/// Small map of 10 string, and 10 numeric properties
// 	Map<String, Object> smallMap2 = null;

// 	/// Medium map of 150 string, and 250 numeric properties
// 	Map<String, Object> mediumMap2 = null;

// 	/// Large map of 1000 string, and 1000 numeric properties
// 	Map<String, Object> largeMap2 = null;

// 	/// Small map of 5 string, and 5 numeric properties
// 	Map<String, Object> smallMapHalf = null;

// 	/// Medium map of 125 string, and 125 numeric properties
// 	Map<String, Object> mediumMapHalf = null;

// 	/// Large map of 500 string, and 500 numeric properties
// 	Map<String, Object> largeMapHalf = null;

// 	/// Iterate and setup a test map, to a given size
// 	public Map<String, Object> setupTestMap(int max) {
// 		HashMap<String, Object> ret = new HashMap<String, Object>();

// 		for (int i = 0; i < max; ++i) {
// 			ret.put("S" + i, GUID.base58());
// 			ret.put("N" + i, Math.pow(1.1, i) * RandomUtils.nextDouble(0, 2.0));
// 		}

// 		return ret;
// 	}

// 	// Number of cols for int, and string respectively
// 	public int smallCols = 50;
// 	public int mediumCols = 200;
// 	public int largeCols = 800;

// 	//Things break from 400 onwards in SQL, too many collumns, args etc.

// 	/// Prepare several test objects for performance testing alter
// 	public void prepareTestObjects() {
// 		smallMap = setupTestMap(smallCols);
// 		mediumMap = setupTestMap(mediumCols);
// 		largeMap = setupTestMap(largeCols);

// 		smallMap2 = setupTestMap(smallCols);
// 		mediumMap2 = setupTestMap(mediumCols);
// 		largeMap2 = setupTestMap(largeCols);

// 		smallMapHalf = setupTestMap(smallCols);
// 		mediumMapHalf = setupTestMap(mediumCols);
// 		largeMapHalf = setupTestMap(largeCols);
// 	}

// 	// Insert / update commands to "implement"
// 	//-----------------------------------------------------

// 	/// Insert an object, and return a GUID.
// 	/// This is intentionally still an upsert to closer replicate actual behaviour in production
// 	public String insert(Map<String, Object> obj) {
// 		String oID = GUID.base58();
// 		jsqlObj.upsert( //
// 			tablename,
// 			//
// 			new String[] { "oID" }, // The unique column names
// 			new Object[] { oID }, // The row unique identifier values
// 			//
// 			new String[] { "jsonData" }, // Columns names to update
// 			new Object[] { ConvertJSON.fromMap(obj) } // Values to update
// 			);
// 		return oID;
// 	}

// 	/// Completely replace an object, with a GUID
// 	public void replace(String guid, Map<String, Object> obj) {
// 		jsqlObj.upsert( //
// 			tablename,
// 			//
// 			new String[] { "oID" }, // The unique column names
// 			new Object[] { guid }, // The row unique identifier values
// 			//
// 			new String[] { "jsonData" }, // Columns names to update
// 			new Object[] { ConvertJSON.fromMap(obj) } // Values to update
// 			);
// 		return;
// 	}

// 	/// Does a delta update of changes, with a GUID and original object for refrence
// 	public void update(String guid, Map<String, Object> ori, Map<String, Object> delta) {
// 		Map<String, Object> val = new HashMap<String, Object>(ori);
// 		val.putAll(delta);

// 		jsqlObj.upsert( //
// 			tablename,
// 			//
// 			new String[] { "oID" }, // The unique column names
// 			new Object[] { guid }, // The row unique identifier values
// 			//
// 			new String[] { "jsonData" }, // Columns names to update
// 			new Object[] { ConvertJSON.fromMap(val) } // Values to update
// 			);
// 		return;
// 	}

// 	// Performance benchmark run
// 	//-----------------------------------------------------

// 	/// Configurable iteration sets count
// 	public int iterationCount = 100;

// 	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 1)
// 	@Test
// 	public void smallMapPerf_insertAndInsert() throws Exception {
// 		for (int i = 0; i < iterationCount; ++i) {
// 			insert(smallMap);
// 			insert(smallMap2);
// 		}
// 	}

// 	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 1)
// 	@Test
// 	public void mediumMapPerf_insertAndInsert() throws Exception {
// 		for (int i = 0; i < iterationCount; ++i) {
// 			insert(mediumMap);
// 			insert(mediumMap2);
// 		}
// 	}

// 	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 1)
// 	@Test
// 	public void largeMapPerf_insertAndInsert() throws Exception {
// 		for (int i = 0; i < iterationCount; ++i) {
// 			insert(largeMap);
// 			insert(largeMap2);
// 		}
// 	}

// 	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 1)
// 	@Test
// 	public void largeMapPerf_insertAndReplace() throws Exception {
// 		for (int i = 0; i < iterationCount; ++i) {
// 			String guid = insert(largeMap);
// 			replace(guid, largeMap2);
// 		}
// 	}

// 	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 1)
// 	@Test
// 	public void mediumMapPerf_insertAndReplace() throws Exception {
// 		for (int i = 0; i < iterationCount; ++i) {
// 			String guid = insert(mediumMap);
// 			replace(guid, mediumMap2);
// 		}
// 	}

// 	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 1)
// 	@Test
// 	public void smallMapPerf_insertAndReplace() throws Exception {
// 		for (int i = 0; i < iterationCount; ++i) {
// 			String guid = insert(smallMap);
// 			replace(guid, smallMap2);
// 		}
// 	}

// 	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 1)
// 	@Test
// 	public void largeMapPerf_insertAndUpdate() throws Exception {
// 		for (int i = 0; i < iterationCount; ++i) {
// 			String guid = insert(largeMap);
// 			update(guid, largeMap, largeMapHalf);
// 		}
// 	}

// 	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 1)
// 	@Test
// 	public void mediumMapPerf_insertAndUpdate() throws Exception {
// 		for (int i = 0; i < iterationCount; ++i) {
// 			String guid = insert(mediumMap);
// 			update(guid, mediumMap, mediumMapHalf);
// 		}
// 	}

// 	@BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 1)
// 	@Test
// 	public void smallMapPerf_insertAndUpdate() throws Exception {
// 		for (int i = 0; i < iterationCount; ++i) {
// 			String guid = insert(smallMap);
// 			update(guid, smallMap, smallMapHalf);
// 		}
// 	}
// }