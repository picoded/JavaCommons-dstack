package picoded.dstack.connector.hazelcast;

import java.util.*;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

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
	public void serverTest() throws Exception {
		// Config map
		GenericConvertMap<String, Object> config = new GenericConvertHashMap<>();
		config.put("groupName", "HazelcastServerConnectorTest");
		
		// Server instance
		HazelcastInstance instance = HazelcastConnector.getConnection(config);
		assertNotNull(instance);
		
		// Close it up
		HazelcastConnector.closeConnection(instance);
	}
	
	@Test
	public void serverClientTest() throws Exception {
		// Config map
		GenericConvertMap<String, Object> config = new GenericConvertHashMap<>();
		config.put("groupName", "HazelcastClientConnectorTest");
		
		// Server instance
		HazelcastInstance server = HazelcastConnector.getConnection(config);
		assertNotNull(server);
		
		// Lets store some data
		Map<String, String> store = server.getMap("hello");
		store.put("world", "one");
		
		// Lets get server info
		Member mem = server.getCluster().getLocalMember();
		assertNotNull(mem);
		
		// Lets get server info
		int port = mem.getAddress().getPort();
		
		// Lets setup client
		GenericConvertMap<String, Object> clientConfig = new GenericConvertHashMap<>();
		clientConfig.put("groupName", "HazelcastClientConnectorTest");
		clientConfig.put("mode", "client");
		clientConfig.put("memberTcpList", "[\"localhost:" + port + "\"]");
		
		// Lets load up the client
		HazelcastInstance client = HazelcastConnector.getConnection(clientConfig);
		assertNotNull(client);
		
		// Lets assert client data
		Map<String, String> read = client.getMap("hello");
		assertEquals("one", read.get("world"));
		
		// Close it up
		HazelcastConnector.closeConnection(client);
		HazelcastConnector.closeConnection(server);
	}
}
