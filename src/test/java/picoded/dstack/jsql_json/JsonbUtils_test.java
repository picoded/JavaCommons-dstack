package picoded.dstack.jsql_json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

// Test dependency
import org.junit.Test;

import picoded.core.struct.MutablePair;
// Test depends
import picoded.dstack.*;
import picoded.dstack.jsql.*;
import picoded.dstack.jsql_json.*;

/**
 * Test specifically utility functions, that are used as foundation blocks for JSQL support
 */
public class JsonbUtils_test {
	
	@Test
	public void jsonQueryPairBuilder_test() {
		
		// Result to be handled
		MutablePair<String, Object[]> res = null;
		
		// Example taken from "queryWithOID" test
		
		// _oid only
		res = JsonbUtils.jsonQueryPairBuilder("_oid = ?", new Object[] { "<unique-oID>" });
		assertNotNull(res);
		assertEquals("oID = ?", res.left);
		
		// _oid with number parmeter
		res = JsonbUtils.jsonQueryPairBuilder("num != ? AND _oid = ?", new Object[] { 1,
			"<unique-oID>" });
		assertNotNull(res);
		assertEquals("((data->>'num')::numeric != ? OR NOT data??'num') AND oID = ?", res.left);
	}
}