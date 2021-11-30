package picoded.dstack.struct.simple;

// Test system include
import static org.junit.Assert.*;
import org.junit.*;
import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;

// Java includes
import java.util.*;

// Test depends
import picoded.core.conv.Base58;
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
	
	// Setup and teardown
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
	
	// Setup of test map
	//-----------------------------------------------------
	
	/// Base62 encoder, and decoder
	Base58 base = Base58.getInstance();

	/**
	 * Iterate and setup a test map, of a given size
	 * This generates a none random map. According to the params
	 * 
	 * @param mapSize   map size to generate
	 * @param itxCount  iteration count, used to seed the values
	 * @return
	 */
	public Map<String, Object> setupTestMap( int mapSize, int itxCount ) {
		HashMap<String, Object> ret = new HashMap<String, Object>();
		
		// Lets setup some of the baseline stuff
		ret.put("ITX", itxCount);

		// Lets add in some random values
		for (int i = 1; i < mapSize; ++i) {
			ret.put("S" + i, base.md5hash("M"+mapSize+"I"+itxCount));
			++i;
			if( i < mapSize ) {
				ret.put("N" + i, mapSize*10000 + itxCount);
			}
		}
		
		return ret;
	}
	
	// Performance benchmark setup
	//-----------------------------------------------------
	
	// Number of cols for int, and string respectively
	public int smallCols = 50;
	public int mediumCols = 200;
	public int largeCols = 400;
	//Things break from 400 onwards in traditional SQL, too many collumns, args etc.
	
	/// Configurable iteration sets count
	public int baseIterationCount = 1000;
	
	// Basic map insert perf benchmark
	//-----------------------------------------------------
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 1)
	@Test
	public void smallMapPerf() throws Exception {
		// Initial setup with maintenance
		mtObj.newEntry( setupTestMap(smallCols, 0) );
		mtObj.maintenance();

		// The actual test benchmarking
		for (int i = 1; i < baseIterationCount; ++i) {
			mtObj.newEntry( setupTestMap(smallCols, i) );
		}
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 1)
	@Test
	public void mediumMapPerf() throws Exception {
		// Initial setup with maintenance
		mtObj.newEntry( setupTestMap(mediumCols, 0) );
		mtObj.maintenance();

		// The actual test benchmarking
		for (int i = 1; i < baseIterationCount; ++i) {
			mtObj.newEntry( setupTestMap(mediumCols, i) );
		}
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 1)
	@Test
	public void largeMapPerf() throws Exception {
		// Initial setup with maintenance
		mtObj.newEntry( setupTestMap(largeCols, 0) );
		mtObj.maintenance();

		// The actual test benchmarking
		for (int i = 1; i < baseIterationCount; ++i) {
			mtObj.newEntry( setupTestMap(largeCols, i) );
		}
	}
	
	// Basic map insert+update perf benchmark
	//-----------------------------------------------------
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 1)
	@Test
	public void smallMapPerf_insertAndUpdate() throws Exception {
		// Initial setup with maintenance
		mtObj.newEntry( setupTestMap(smallCols, 0) );
		mtObj.maintenance();

		// The actual test benchmarking
		for (int i = 1; i < baseIterationCount; ++i) {
			DataObject mo = mtObj.newEntry( setupTestMap(smallCols, -i) );
			mo.saveDelta();
			mo.putAll( setupTestMap(smallCols, i) );
			mo.saveDelta();
		}
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 1)
	@Test
	public void mediumMapPerf_insertAndUpdate() throws Exception {
		// Initial setup with maintenance
		mtObj.newEntry( setupTestMap(mediumCols, 0) );
		mtObj.maintenance();

		// The actual test benchmarking
		for (int i = 1; i < baseIterationCount; ++i) {
			DataObject mo = mtObj.newEntry( setupTestMap(mediumCols, -i) );
			mo.saveDelta();
			mo.putAll( setupTestMap(mediumCols, i) );
			mo.saveDelta();
		}
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 1)
	@Test
	public void largeMapPerf_insertAndUpdate() throws Exception {
		// Initial setup with maintenance
		mtObj.newEntry( setupTestMap(largeCols, 0) );
		mtObj.maintenance();

		// The actual test benchmarking
		for (int i = 1; i < baseIterationCount; ++i) {
			DataObject mo = mtObj.newEntry( setupTestMap(largeCols, -i) );
			mo.saveDelta();
			mo.putAll( setupTestMap(largeCols, i) );
			mo.saveDelta();
		}
	}
	
	// Basic map insert & query perf benchmark
	//-----------------------------------------------------
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 1)
	@Test
	public void smallMapQueryPerf() throws Exception {
		// Initial setup of map
		smallMapPerf();

		// Lets perform the iteration query
		for (int i = 1; i < baseIterationCount-2; ++i) {
			assertEquals( baseIterationCount-i, mtObj.queryCount("ITX >= ?", new Object[] { i }));
		}
	}
	
	@BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 1)
	@Test
	public void mediumMapQueryPerf() throws Exception {
		// Initial setup of map
		mediumMapPerf();

		// Lets perform the iteration query
		for (int i = 1; i < baseIterationCount-2; ++i) {
			assertEquals( baseIterationCount-i, mtObj.queryCount("ITX >= ?", new Object[] { i }));
		}
	}
	
	
}