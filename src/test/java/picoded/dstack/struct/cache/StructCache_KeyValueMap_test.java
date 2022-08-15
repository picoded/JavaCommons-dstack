package picoded.dstack.struct.cache;

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
import picoded.dstack.jsql.*;
import picoded.dstack.struct.simple.*;

public class StructCache_KeyValueMap_test extends StructSimple_KeyValueMap_test {
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Impomentation constructor
	public KeyValueMap implementationConstructor() {
		StructCache_KeyValueMap map = new StructCache_KeyValueMap();
		map.configMap().put("name", JSqlTestConfig.randomTablePrefix());
		return map;
	}
	
}
