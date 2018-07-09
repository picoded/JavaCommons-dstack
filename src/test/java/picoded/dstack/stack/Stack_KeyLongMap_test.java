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

public class Stack_KeyLongMap_test extends StructSimple_KeyLongMap_test {
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Top layer DataObjectMap implmentation used
	public Core_KeyLongMap layer1;
	
	/// Impomentation constructor for stack setup
	public KeyLongMap implementationConstructor() {
		layer1 = new StructSimple_KeyLongMap();
		return new Stack_KeyLongMap(new Core_KeyLongMap[] { layer1 });
	}
	
}
