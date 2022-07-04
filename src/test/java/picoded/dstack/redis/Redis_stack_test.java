package picoded.dstack.redis;

// Test system include
import static org.junit.Assert.*;
import org.junit.*;

import picoded.core.struct.*;
import picoded.dstack.*;
import picoded.dstack.struct.simple.*;

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
public class Redis_stack_test {
	
	protected static volatile RedisStack instance = null;
	
	@Test
	public void stackTest() {
		
		GenericConvertMap<String, Object> redisConfig = new GenericConvertHashMap<>();
		redisConfig.put("host", DStackTestConfig.REDIS_HOST());
		redisConfig.put("port", DStackTestConfig.REDIS_PORT());
		
		redisConfig.put("name", DStackTestConfig.randomTablePrefix());
		
		GenericConvertMap<String, Object> stackConfig = new GenericConvertHashMap<>();
		stackConfig.put("name", "Redis_DataObjectMap_test");
		stackConfig.put("redis", redisConfig);
		
		instance = new RedisStack(stackConfig);
		
        //Test instance instanciation :o)
		assertSame("pong", instance.ping());
		
		RedissonClient redisson = null;
        redisson = instance.getConnection();
		assertNotNull(redisson);

        //Test that redisson client works properly because i'm paranoid
        RBucket<String> bucket = redisson.getBucket("stringObject");
		bucket.set("hello world");
		String objValue = bucket.get();
		assertEquals(objValue, "hello world");

        //Clear current db of from all keys
		redisson.getKeys().flushdb();
		//Shutdown client connection
		redisson.shutdown();
	}
	
}