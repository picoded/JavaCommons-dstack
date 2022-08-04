package picoded.dstack.redis;

import picoded.core.struct.*;
import picoded.dstack.*;
import picoded.dstack.struct.simple.*;

import java.util.concurrent.ThreadLocalRandom;

<<<<<<< HEAD
public class Redis_KeyLongMap_test extends StructSimple_KeyLongMap_test {
=======
public class RedisKeyLongMap_test extends StructSimple_KeyLongMap_test {
>>>>>>> ba5858da6960b26e62926ee51c1e3f547b58774f
	// Redis stack instance
	protected static volatile RedisStack instance = null;
	
	/// Implementation constructor
	public KeyLongMap implementationConstructor() {
		
		// Initialize server
<<<<<<< HEAD
		synchronized (Redis_KeyLongMap_test.class) {
=======
		synchronized (RedisKeyLongMap_test.class) {
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
				stackConfig.put("name", "RedisKeyLongMap_test");
				stackConfig.put("redis", redisConfig);
				
				instance = new RedisStack(stackConfig);
			}
		}
		// Load the KeyLongMap
<<<<<<< HEAD
		return instance.keyLongMap(DStackTestConfig.randomTablePrefix());
	}
}
=======
		return instance.KeyLongMap(DStackTestConfig.randomTablePrefix());
	}
}
>>>>>>> ba5858da6960b26e62926ee51c1e3f547b58774f
