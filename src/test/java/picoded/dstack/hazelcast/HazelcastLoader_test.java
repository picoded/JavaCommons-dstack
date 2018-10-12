package picoded.dstack.hazelcast;

import java.util.*;

// junit include
import org.junit.*;
import static org.junit.Assert.*;

/**
 * Hello loading of hazelcast server, and closing it
 */
public class HazelcastLoader_test {
	
	@Test
	public void sanityTest() throws Exception {
		assertNotNull(HazelcastLoader.getConnection("sanity-test", new HashMap<String, Object>()));
		HazelcastLoader.closeConnection("sanity-test");
	}
}