package picoded.dstack.redis;

import picoded.core.struct.*;
import picoded.dstack.*;
import picoded.dstack.struct.simple.*;

import java.util.concurrent.ThreadLocalRandom;

public class Redis_KeyLongMap_test extends StructSimple_KeyLongMap_test {
	// Redis stack instance
	protected static volatile RedisStack instance = null;
	
	/// Implementation constructor
	public KeyLongMap implementationConstructor() {
		
		// Initialize server
		synchronized (Redis_KeyLongMap_test.class) {
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
				stackConfig.put("name", "RedisKeyLongMap_test");
				stackConfig.put("redis", redisConfig);
				
				instance = new RedisStack(stackConfig);
			}
		}
		// Load the KeyLongMap
		return instance.keyLongMap(DStackTestConfig.randomTablePrefix());
	}
}