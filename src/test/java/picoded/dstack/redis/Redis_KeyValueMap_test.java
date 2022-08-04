package picoded.dstack.redis;

import picoded.core.struct.*;
import picoded.dstack.*;
import picoded.dstack.struct.simple.*;

import java.util.concurrent.ThreadLocalRandom;


<<<<<<< HEAD
public class Redis_KeyValueMap_test extends StructSimple_KeyValueMap_test {
=======
public class RedisKeyValueMap_test extends StructSimple_KeyValueMap_test {
>>>>>>> ba5858da6960b26e62926ee51c1e3f547b58774f
	// Redis stack instance
	protected static volatile RedisStack instance = null;
	
	/// Implementation constructor
	public KeyValueMap implementationConstructor() {
		
		// Initialize server
<<<<<<< HEAD
		synchronized (Redis_KeyValueMap_test.class) {
=======
		synchronized (RedisKeyValueMap_test.class) {
>>>>>>> ba5858da6960b26e62926ee51c1e3f547b58774f
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
				stackConfig.put("name", "RedisKeyValueMap_test");
				stackConfig.put("redis", redisConfig);
				
				instance = new RedisStack(stackConfig);
			}
		}
		// Load the KeyValueMap
		return instance.keyValueMap(DStackTestConfig.randomTablePrefix());
	}
<<<<<<< HEAD
}
=======
}
>>>>>>> ba5858da6960b26e62926ee51c1e3f547b58774f
