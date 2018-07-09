package picoded.dstack.stack;

import static org.junit.Assert.assertArrayEquals;
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

public class Stack_FileWorkspaceMap_test extends StructSimple_FileWorkspaceMap_test {
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Top layer Stack_FileWorkspaceMap implmentation used
	public Core_FileWorkspaceMap layer1;
	
	/// Secondary layer Stack_FileWorkspaceMap implmentation used
	public Core_FileWorkspaceMap layer2;
	
	/// Impomentation constructor for stack setup
	public FileWorkspaceMap implementationConstructor() {
		layer1 = new StructSimple_FileWorkspaceMap();
		layer2 = new StructSimple_FileWorkspaceMap();
		return new Stack_FileWorkspaceMap(new Core_FileWorkspaceMap[] { layer1, layer2 });
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
		FileWorkspace data = layer2.newEntry();
		data.writeByteArray("hello", "world".getBytes());
		String oid = data._oid();
		
		// Gettign from the stack
		data = testObj.get(oid);
		assertNotNull(data);
		assertArrayEquals("world".getBytes(), data.readByteArray("hello"));
	}
}
