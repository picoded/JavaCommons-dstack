package picoded.dstack.jsql.perf;

import java.util.*;
import picoded.dstack.jsql.connector.*;
import picoded.dstack.jsql.*;
import picoded.dstack.jsql.JSqlTestConfig;

/// Testing of DataTable obje-key-val, in batchless mode
public class JSqlObjKeyValBatchless_perf extends JSqlObjKeyVal_perf {
	
	/// Does a delta update of changes, with a GUID and original object for refrence
	public void update(String guid, Map<String, Object> ori, Map<String, Object> delta) {
		// Delta keys to iterate
		String[] deltaKeys = delta.keySet().toArray(new String[0]);
		
		// For each key, upsert
		for (int i = 0; i < deltaKeys.length; ++i) {
			// attribute by attribute upsert
			jsqlObj.upsert( //
				tablename, // Table name to upsert on
				//
				new String[] { "oID", "kID" }, new Object[] { guid, deltaKeys[i] }, // The row unique identifier values
				//
				new String[] { "sVl" }, // Columns names to update
				new Object[] { delta.get(deltaKeys[i]) }, // Values to update
				// Columns names to apply default value, if not exists
				// Values to insert, that is not updated. Note that this is ignored if pre-existing values exists
				null, //
				null, //
				// Various column names where its existing value needs to be maintained (if any),
				// this is important as some SQL implementation will fallback to default table values, if not properly handled
				null //
				);
		}
		
		return;
	}
}