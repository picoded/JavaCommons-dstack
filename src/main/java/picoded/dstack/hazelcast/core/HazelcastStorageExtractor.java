package picoded.dstack.hazelcast.core;

import picoded.core.conv.NestedObjectFetch;
import picoded.core.conv.StringEscape;

import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;

/**
 * Hazelcast custom map attribute extractor, this is one major work around the lack of 
 * map query support which is pending merge here : https://github.com/hazelcast/hazelcast/pull/12708
 * 
 * The current work around, would be to initialize an ValueExtractor, with the following attribute config
 * (Adapted from https://docs.hazelcast.org/docs/latest-development/manual/html/Distributed_Query/Custom_Attributes/Configuring_a_Custom_Attribute_Programmatically.html)
 * 
 * ```
 * MapConfig mapConfig = new MapConfig();
 * mapConfig.addMapAttributeConfig( new MapAttributeConfig("self", "picoded.dstack.hazelcast.HazelcastStorageExtractor") );
 * ```
 * 
 * Subsequently then, when one would do a query for `hello = ?`, it would then be transformed into `self[hello] = ?`
 * This applies to query indexes as well.
 */
public class HazelcastStorageExtractor implements ValueExtractor<HazelcastStorageMap, String> {
	/**
	 * Extractor which recieves the storage map, and the parameter string 
	 */
	public void extract(HazelcastStorageMap target, String argument, ValueCollector collector) {
		// Decode the URI
		String arg = StringEscape.decodeURI(argument);
		
		// Get the result, fetch it if needed
		Object res = target.get(arg);
		if (res == null) {
			res = NestedObjectFetch.fetchObject(target, arg, null);
		}
		
		// Add result into the collector
		collector.addObject(res);
	}
}
