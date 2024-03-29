package picoded.dstack.redisson;

import picoded.core.struct.*;
import picoded.dstack.*;
import picoded.dstack.struct.simple.*;

import java.util.concurrent.ThreadLocalRandom;

/**
 * ## Purpose
 * This class is meant to test the Redisson_KeyValueMap implementation,
 * and ensure that it passes all the test layed out in StructSimple_KeyValueMap_test
 * 
 */
public class Redisson_KeyValueMap_test extends StructSimple_KeyValueMap_test {
	// Redis stack instance
	protected static volatile RedissonStack instance = null;
	
	/// Implementation constructor
	public KeyValueMap implementationConstructor() {
		
		// Initialize server
		synchronized (Redisson_KeyValueMap_test.class) {
			if (instance == null) {
				// The default config uses "172.17.0.1" (default docker bridge address) 
				// and port 6379 (default redis port)
				GenericConvertMap<String, Object> redisConfig = new GenericConvertHashMap<>();
				redisConfig.put("host", DStackTestConfig.REDIS_HOST());
				redisConfig.put("port", DStackTestConfig.REDIS_PORT());
				
				// Use a random DB number between 0 and 15.
				// int randomNum = ThreadLocalRandom.current().nextInt(0, 15 + 1);
				redisConfig.put("name", DStackTestConfig.randomTablePrefix());
				
				GenericConvertMap<String, Object> stackConfig = new GenericConvertHashMap<>();
				stackConfig.put("name", "Redisson_KeyValueMap_test");
				stackConfig.put("redis", redisConfig);
				
				instance = new RedissonStack(stackConfig);
			}
		}
		// Load the KeyValueMap
		return instance.keyValueMap(DStackTestConfig.randomTablePrefix());
	}
}