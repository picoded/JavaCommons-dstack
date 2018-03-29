package picoded.dstack.jsql.perf;

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
import org.apache.commons.lang3.ArrayUtils;

import picoded.core.conv.ConvertJSON;
// Test depends
import picoded.core.conv.GUID;
import picoded.core.struct.CaseInsensitiveHashMap;
import picoded.dstack.jsql.connector.*;
import picoded.dstack.jsql.*;
import picoded.dstack.jsql.JSqlTestConfig;

/// Testing of DataTable full indexless fixed table performance
public class JSqlFixedIndex_perf extends JSqlFixedIndexless_perf {
	
	//public String[] collumnNames = null;
	
	public void tableSetup() {
		// Does original table setup
		super.tableSetup();
		
		// This optimizes query by collumn names
		for (int i = 0; i < collumnNames.length; ++i) {
			jsqlObj.createIndex( //
				tablename, collumnNames[i], null, collumnNames[i] //
				); //
		}
	}
}