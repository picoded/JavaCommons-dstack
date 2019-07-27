package picoded.dstack.hazelcast.core;

import java.util.HashMap;

import picoded.core.conv.GenericConvert;

/**
 * Custom Comparable hazelcast map used for data storage 
 */
public class HazelcastStorageMap extends HashMap<String, Object> implements
	Comparable<HazelcastStorageMap> {
	// Disagree on why this is needed, but required for serialization support =|
	static final long serialVersionUID = 1L;
	
	/** 
	 * @param o HazelcastStorageMap to compare against
	 * @return comparable by _oid property 
	 **/
	public int compareTo(HazelcastStorageMap o) {
		String this_oid = GenericConvert.toString(this.get("_oid"), "");
		String obj_oid = GenericConvert.toString(o.get("_oid"), "");
		return this_oid.compareTo(obj_oid);
	}
}
