package picoded.dstack.hazelcast;

import picoded.dstack.*;
import picoded.dstack.jsql.JSqlTestConfig;
import picoded.dstack.struct.simple.*;
import java.util.HashMap;

public class Hazelcast_KeyValueMap_test extends StructSimple_KeyValueMap_test {
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Impomentation constructor
	public KeyValueMap implementationConstructor() {
		Hazelcast_KeyValueMap map = new Hazelcast_KeyValueMap( //
			HazelcastLoader.getConnection("Hazelcast_KeyValueMap_test", new HashMap<String, Object>()), //
			JSqlTestConfig.randomTablePrefix() //
		); //
		return map;
	}
}
