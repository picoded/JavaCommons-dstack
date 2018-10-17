package picoded.dstack.hazelcast;

// Test depends
import picoded.dstack.*;
import picoded.dstack.jsql.JSqlTestConfig;
import picoded.dstack.struct.simple.*;
import java.util.HashMap;

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
