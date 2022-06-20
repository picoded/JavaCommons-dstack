package picoded.dstack.redis;

// Test system include
import static org.junit.Assert.*;
import org.junit.*;

import picoded.dstack.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.redisson.config.Config;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.api.RKeys;
import org.redisson.api.RType;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;

import org.redisson.api.RAtomicLong;

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
		RedissonClient redisson = Redisson.create(config);
		assertNotNull(redisson);
		
		// perform basic operations
		RBucket<String> bucket = redisson.getBucket("stringObject");
		bucket.set("hello world");
		RMap<String, String> map = redisson.getMap("mapObject");
		map.put("helloKey", "worldValue");
		String objValue = bucket.get();
		assertEquals(objValue, "hello world");
		String mapValue = map.get("helloKey");
		assertEquals(mapValue, "worldValue");
		
		//Set value, modify it then remove it in Redis
		//Set AtomicLong key/value in Redis
		String myKey = "myLong";
		RAtomicLong atomicLong = redisson.getAtomicLong(myKey);
		atomicLong.set(5);
		assertEquals("5", atomicLong.toString());
		//Increment value and check result
		atomicLong.addAndGet(15);
		assertEquals("20", atomicLong.toString());
		//Delete Key
		redisson.getKeys().delete(atomicLong);
		
		//Check keys doesn't exist anymore
		RType type = redisson.getKeys().getType(myKey);
		assertEquals(null, type);
		
		//Clear current db of from all keys
		redisson.getKeys().flushdb();
		//Shutdown client connection
		redisson.shutdown();
	}
	
}