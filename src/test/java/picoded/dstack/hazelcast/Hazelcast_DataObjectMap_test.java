package picoded.dstack.hazelcast;

// Target test class
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;

// Test Case include
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// Test depends
import picoded.dstack.*;
import picoded.dstack.jsql.*;
import picoded.dstack.jsql.connector.*;
import picoded.dstack.struct.simple.*;

public class Hazelcast_DataObjectMap_test extends StructSimple_DataObjectMap_test {
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Impomentation constructor
	public DataObjectMap implementationConstructor() {
		Hazelcast_DataObjectMap map = new Hazelcast_DataObjectMap( //
			HazelcastLoader.getConnection("Hazelcast_DataObjectMap_test",
				new HashMap<String, Object>()), //
			JSqlTestConfig.randomTablePrefix() //
		); //
		return map;
	}
	
}
