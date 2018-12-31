package picoded.dstack.connector.hazelcast;

import java.util.*;

import com.hazelcast.core.HazelcastInstance;

// junit include
import org.junit.*;

import picoded.core.struct.GenericConvertHashMap;
import picoded.core.struct.GenericConvertMap;

import static org.junit.Assert.*;

/**
 * Hello loading of hazelcast server, and closing it
 */
public class HazelcastConnector_test {
	
	@Test
	public void sanityTest() throws Exception {
		// Config map
		GenericConvertMap<String, Object> config = new GenericConvertHashMap<>();
		config.put("groupName", "SanityTest");
		
		// Server instance
		HazelcastInstance instance = HazelcastConnector.getConnection(config);
		assertNotNull(instance);
		HazelcastConnector.closeConnection(instance);
	}
}