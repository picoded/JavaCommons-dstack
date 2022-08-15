package picoded.dstack.struct.cache;

import picoded.core.struct.GenericConvertMap;

import java.util.concurrent.TimeUnit;

// Cache2k implmentation
import org.cache2k.Cache2kBuilder;
import org.cache2k.Cache;

class StructCacheUtil {
	
	/**
	 * Utility function used to build a new Cache2k cache instance
	 * This handles all the various common config settings, and set it up accordingly
	 */
	static <V> Cache<String, V> setupCache2kMap(Cache2kBuilder<String,V> builder, String name, GenericConvertMap<String, Object> config) {
		
		//
		// Get Config
		//-----------------------------------------------------------------------
		
		//
		// Alright, time to build a new cache
		// We are in the era of GB ram computing, 10k cache would
		// be a good sane default in server environment. Even if there are 
		// multiple sets of StructCache, as it would take ~60MB each
		//
		// That means, with 10 data set being cached, that would be about ~600MB
		//
		// to consider : auto detect RAM size in KB - and use that?
		// a good rough guideline would be 1/4 of free ram space divided by 6kb
		// as a capcity size auto detction
		//
		// # DataObjectMap caching napkin math assumptions
		// - Assume a hashmap object with 30 parameters (including system keys)
		// - Because its hard to predict the capacity/size ratio it is assumed to be 1:1
		// - Keys and value storage are assumed to be a 22 character string
		//
		// > The above assumptions was designed to somewhat be the upper limit of 
		// > ram storage cost for a data object map. Rather then an average.
		//
		// # References
		// - http://java-performance.info/memory-consumption-of-java-data-types-2/
		// - https://www.javamex.com/tutorials/memory/string_memory_usage.shtml
		//
		// # The Math
		//
		//   36 bytes : 32+4 bytes - HashMap space on primary cache map
		//  108 bytes : 3 x overhead for cache mapping
		//   62 bytes : 40 overhead + 22 oid string key
		// 1080 bytes : 30 x (32+4)  HashMap overhead
		// 1860 bytes : 30 x (40+22) ObjectMap key strings
		// 1860 bytes : 30 x (40+22) ObjectMap value strings
		// ----------
		// 5006 bytes : Total bytes per object map
		// ~ 6 kilo bytes : Rounded up
		//
		// # RAM cost for 10k objects
		//
		// 10,000 * 6 KB = 60 MB
		//
		// > So yea, we are ok to assume a 10k objects for most parts
		//
		int capacity = config.getInt("capacity", 10000);
		
		// // Disable monitoring statistics by default
		// boolean monitoring = config.getBoolean("monitoring", false);
		
		// Optimize for high concurrency mode, not recommended unless its
		// >100k size, and 8 vCPU that is constantly under heavy load
		boolean boostConcurrency = config.getBoolean("boostConcurrency", false);
		
		// Enable / Disable statistics, because we do not provide any API interface for this
		// there is almost no use case, unless you are bypassing our api and modifying cach2k directly
		boolean statistics = config.getBoolean("statistics", false);
		
		// Number of milliseconds to cache the values up to, note that this should operate independent of 
		// any expiry logic that is used for KeyValueMap, defaults to 3 days (in seconds)
		//
		// If you want to disable expiry (which makes no sense), use -1
		long valueLifespan = config.getLong("valueLifespan", 3 * 24 * 60 * 60 * 1000L);
		
		//
		// Setup the builder
		//-----------------------------------------------------------------------
		
		// Configure cache name and capacity
		builder = builder.name(name).entryCapacity(capacity);
		
		// // Disable monitoring
		// if (monitoring == false) {
		// 	builder = builder.disableMonitoring(true);
		// }
		// boostConcurrency if configured
		if (boostConcurrency == true) {
			builder = builder.boostConcurrency(true);
		}
		if (statistics == false) {
			builder = builder.disableStatistics(true);
		}
		if (valueLifespan >= 0) {
			builder = builder.expireAfterWrite(valueLifespan, TimeUnit.MILLISECONDS);
		} else {
			builder = builder.eternal(true);
		}
		
		//
		// Build and return the built cache
		//-----------------------------------------------------------------------
		return builder.build();
	}
}