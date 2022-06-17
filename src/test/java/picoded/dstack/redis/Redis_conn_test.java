package picoded.dstack.mongodb;

// Test system include
import static org.junit.Assert.*;
import org.junit.*;

import picoded.dstack.*;

import org.redisson.Redisson;
import org.redisson.config.Config;
import org.redisson.api.RedissonClient;
import org.redisson.api.RKeys;

/**
 * Minimal Redis connectivity test.
 * Used to test assumptions used to build Redis implementation
 */
public class Redis_conn_test {
	
	@Test
	public void connectivityTest() {
		// Get the full_url
		String full_url = "redis://" + DStackTestConfig.REDIS_HOST() + ":"
			+ DStackTestConfig.REDIS_PORT() + "/0";
		System.out.println(full_url);
		
		// Lets build using Redisson Config
		Config config = new Config();
		config.useSingleServer()
		// use "rediss://" for SSL connection
			.setAddress(full_url);
		
		// The mongodb client
		RedissonClient client = Redisson.create(config);
		assertNotNull(client);
		
		RKeys keys = client.getKeys();
		System.out.println("Hello, World!" + keys);
		
	}
	
}