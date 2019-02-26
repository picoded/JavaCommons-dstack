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
// import org.apache.commons.lang3.ArrayUtils;

// import picoded.core.conv.ConvertJSON;
// // Test depends
// import picoded.core.conv.GUID;
// import picoded.core.struct.CaseInsensitiveHashMap;
// import picoded.dstack.jsql.connector.*;
// import picoded.dstack.jsql.*;
// import picoded.dstack.jsql.JSqlTestConfig;

// /// Testing of DataTable full indexless fixed table performance
// public class JSqlObjKeyVal_perf extends JSqlClob_perf {

// 	public void tableSetup() {

// 		jsqlObj.createTable( //
// 			tablename, new String[] { "pKy", "oID", "kID", "sVl" }, new String[] {
// 				"BIGINT PRIMARY KEY AUTOINCREMENT", "VARCHAR(64)", "VARCHAR(64)", "VARCHAR(64)" });

// 		// This optimizes query by object keys,
// 		// with the following combinations
// 		// + oID
// 		// + oID, kID
// 		// + oID, kID, idx
// 		jsqlObj.createIndex( //
// 			tablename, "oID, kID", "UNIQUE", "unq" //
// 		); //

// 		// This optimizes for string values
// 		// + kID
// 		// + kID, sVl
// 		jsqlObj.createIndex( //
// 			tablename, "kID, sVl", null, "ksIdx" //
// 		); //

// 	}

// 	// Insert / update commands to "implement"
// 	//-----------------------------------------------------

// 	/// Extract values from a map to an array
// 	public Object[] extractValueFromMap(String[] names, Map<String, Object> obj) {
// 		Object[] ret = new Object[names.length];
// 		for (int i = 0; i < names.length; ++i) {
// 			ret[i] = obj.get(names[i]);
// 		}
// 		return ret;
// 	}

// 	/// Insert an object, and return a GUID.
// 	/// This is intentionally still an upsert to closer replicate actual behaviour in production
// 	public String insert(Map<String, Object> obj) {
// 		String oID = GUID.base58();
// 		update(oID, null, obj);
// 		return oID;
// 	}

// 	/// Completely replace an object, with a GUID
// 	public void replace(String guid, Map<String, Object> obj) {
// 		update(guid, null, obj);
// 		return;
// 	}

// 	/// Does a delta update of changes, with a GUID and original object for refrence
// 	public void update(String guid, Map<String, Object> ori, Map<String, Object> delta) {
// 		// Delta keys to iterate
// 		String[] deltaKeys = delta.keySet().toArray(new String[0]);

// 		// The insert values 
// 		List<Object[]> uniqueValuesList = new ArrayList<Object[]>();
// 		List<Object[]> insertValuesList = new ArrayList<Object[]>();

// 		for (int i = 0; i < deltaKeys.length; ++i) {
// 			uniqueValuesList.add(new Object[] { guid, deltaKeys[i] });
// 			insertValuesList.add(new Object[] { delta.get(deltaKeys[i]) });
// 		}

// 		jsqlObj.multiUpsert( //
// 			tablename, // Table name to upsert on
// 			//
// 			new String[] { "oID", "kID" }, uniqueValuesList, // The row unique identifier values
// 			//
// 			new String[] { "sVl" }, // Columns names to update
// 			insertValuesList, // Values to update
// 			// Columns names to apply default value, if not exists
// 			// Values to insert, that is not updated. Note that this is ignored if pre-existing values exists
// 			null, //
// 			null, //
// 			// Various column names where its existing value needs to be maintained (if any),
// 			// this is important as some SQL implementation will fallback to default table values, if not properly handled
// 			null //
// 			);

// 		return;
// 	}

// }