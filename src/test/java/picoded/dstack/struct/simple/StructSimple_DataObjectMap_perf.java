package picoded.dstack.struct.simple;

// Test system include
import static org.junit.Assert.*;
import org.junit.*;
import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;

// Java includes
import java.util.*;

// External lib includes
import org.apache.commons.lang3.RandomUtils;

// Test depends
import picoded.core.conv.GUID;
import picoded.core.struct.CaseInsensitiveHashMap;
import picoded.dstack.*;
import picoded.dstack.struct.simple.*;

// DataObjectMap base test class
public class StructSimple_DataObjectMap_perf extends AbstractBenchmark {
	
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
		
		prepareTestObjects();
	}
	
	@After
	public void tearDown() {
		if (mtObj != null) {
			mtObj.systemDestroy();
		}
		mtObj = null;
	}
	
	// Performance benchmark setup
	//-----------------------------------------------------
	
	// Number of cols for int, and string respectively
	public int smallCols = 50;
	public int mediumCols = 200;
	public int largeCols = 800;
	//Things break from 400 onwards in SQL, too many collumns, args etc.
	
	/// Small map of 10 string, and 10 numeric properties
	Map<String, Object> smallMap = null;
	
	/// Medium map of 200 string, and 250 numeric properties
	Map<String, Object> mediumMap = null;
	
	/// Large map of 1000 string, and 1000 numeric properties
	Map<String, Object> largeMap = null;
	
	/// Small map of 10 string, and 10 numeric properties
	Map<String, Object> smallMap2 = null;
	
	/// Medium map of 200 string, and 250 numeric properties
	Map<String, Object> mediumMap2 = null;
	
	/// Large map of 1000 string, and 1000 numeric properties
	Map<String, Object> largeMap2 = null;
	
	/// Iterate and setup a test map, to a given size
	public Map<String, Object> setupTestMap(int max) {
		HashMap<String, Object> ret = new HashMap<String, Object>();
		
		for (int i = 0; i < max; ++i) {
			ret.put("S" + i, GUID.base58());
			ret.put("N" + i, Math.pow(1.1, i) * RandomUtils.nextDouble(0, 2.0));
		}
		
		return ret;
	}
	
	/// Prepare several test objects for performance testing alter
	public void prepareTestObjects() {
		smallMap = setupTestMap(smallCols);
		mediumMap = setupTestMap(mediumCols);
		largeMap = setupTestMap(largeCols);
		smallMap2 = setupTestMap(smallCols);
		mediumMap2 = setupTestMap(mediumCols);
		largeMap2 = setupTestMap(largeCols);
	}
	
	/// Configurable iteration sets count
	public int iterationCount = 100;
	
	@BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 1)
	@Test
	public void largeMapPerf() throws Exception {
		for (int i = 0; i < iterationCount; ++i) {
			mtObj.newEntry(largeMap);
			mtObj.newEntry(largeMap2);
		}
	}
	
	@BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 1)
	@Test
	public void mediumMapPerf() throws Exception {
		for (int i = 0; i < iterationCount; ++i) {
			mtObj.newEntry(mediumMap);
			mtObj.newEntry(mediumMap2);
		}
	}
	
	@BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 1)
	@Test
	public void smallMapPerf() throws Exception {
		for (int i = 0; i < iterationCount; ++i) {
			mtObj.newEntry(smallMap);
			mtObj.newEntry(smallMap2);
		}
	}
	
	@BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 1)
	@Test
	public void largeMapPerf_insertAndUpdate() throws Exception {
		for (int i = 0; i < iterationCount; ++i) {
			DataObject mo = mtObj.newEntry(largeMap);
			mo.saveDelta();
			
			mo.putAll(largeMap2);
			mo.saveDelta();
		}
	}
	
	@BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 1)
	@Test
	public void mediumMapPerf_insertAndUpdate() throws Exception {
		for (int i = 0; i < iterationCount; ++i) {
			DataObject mo = mtObj.newEntry(mediumMap);
			mo.saveDelta();
			
			mo.putAll(mediumMap2);
			mo.saveDelta();
		}
	}
	
	@BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 1)
	@Test
	public void smallMapPerf_insertAndUpdate() throws Exception {
		for (int i = 0; i < iterationCount; ++i) {
			DataObject mo = mtObj.newEntry(smallMap);
			mo.saveDelta();
			
			mo.putAll(smallMap2);
			mo.saveDelta();
		}
	}
	
}