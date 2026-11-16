package picoded.dstack.stack;

// Target test class
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;

// Test Case include
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// Test depends
import picoded.dstack.*;
import picoded.dstack.core.*;
import picoded.dstack.struct.simple.*;
import picoded.dstack.stack.Stack_DataObjectMap;

public class Stack_DataObjectMap_test extends StructSimple_DataObjectMap_test {
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Top layer DataObjectMap implmentation used
	public Core_DataObjectMap layer1;
	
	/// Secondary layer DataObjectMap implmentation used
	public Core_DataObjectMap layer2;
	
	/// Impomentation constructor for stack setup
	public DataObjectMap implementationConstructor() {
		layer1 = new StructSimple_DataObjectMap();
		layer2 = new StructSimple_DataObjectMap();
		return new Stack_DataObjectMap(new Core_DataObjectMap[] { layer1, layer2 });
	}
	
	//--------------------------------------------------------------------------
	//
	// Layered testing
	//
	//--------------------------------------------------------------------------
	
	/// Lower layered read
	@Test
	public void lowerLayerRead() {
		// Setup the lower layer
		DataObject data = layer2.newEntry();
		data.put("hello", "world");
		data.saveAll();
		String oid = data._oid();
		
		// Validate upper layer does not have data
		assertNull(layer1.get(oid));
		
		// Gettign from the stack
		data = mtObj.get(oid);
		assertNotNull(data);
		assertEquals("world", data.get("hello"));
	}

	@Test
	public void testBulkRemoveAcrossLayers() {
		// Populate item in bottom layer
		DataObject data1 = layer2.newEntry();
		data1.put("k", "v1");
		data1.saveAll();
		String oid1 = data1._oid();

		// Populate item directly in top layer
		DataObject data2 = layer1.newEntry();
		data2.put("k", "v2");
		data2.saveAll();
		String oid2 = data2._oid();

		// Read data1 through stack so it is cached in layer1
		DataObject readData1 = mtObj.get(oid1);
		assertNotNull(readData1);
		assertNotNull(layer1.get(oid1));
		assertNotNull(layer2.get(oid1));

		// Bulk remove both IDs through stack
		long removed = mtObj.removeBulk(Arrays.asList(oid1, oid2));
		assertTrue(removed >= 0);

		// Assert both layers no longer have the items
		assertNull(layer1.get(oid1));
		assertNull(layer2.get(oid1));
		assertNull(layer1.get(oid2));
		assertNull(mtObj.get(oid1));
		assertNull(mtObj.get(oid2));
	}
}
