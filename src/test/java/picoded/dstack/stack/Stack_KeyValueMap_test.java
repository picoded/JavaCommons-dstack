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
import picoded.dstack.stack.*;

public class Stack_KeyValueMap_test extends StructSimple_KeyValueMap_test {
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Top layer DataObjectMap implmentation used
	public Core_KeyValueMap layer1;
	
	/// Secondary layer DataObjectMap implmentation used
	public Core_KeyValueMap layer2;
	
	/// Impomentation constructor for stack setup
	public KeyValueMap implementationConstructor() {
		layer1 = new StructSimple_KeyValueMap();
		layer2 = new StructSimple_KeyValueMap();
		return new Stack_KeyValueMap(new Core_KeyValueMap[] { layer1, layer2 });
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
		layer2.put("hello", "world");
		assertEquals("world", testObj.getString("hello"));
		
		// Setup the upper layer
		layer1.put("hello", "meow");
		assertEquals("meow", testObj.getString("hello"));
	}
}
