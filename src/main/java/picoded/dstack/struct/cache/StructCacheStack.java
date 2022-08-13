package picoded.dstack.struct.cache;

import picoded.core.struct.GenericConvertMap;
import picoded.dstack.core.*;
import picoded.dstack.struct.simple.*;
import picoded.dstack.*;

/**
 * [Internal use only]
 * 
 * StructCache configuration based stack provider
 **/
public class StructCacheStack extends CoreStack {
	
	/**
	 * Constructor with configuration map
	 */
	public StructCacheStack(GenericConvertMap<String, Object> inConfig) {
		super(inConfig);
	}
	
	/**
	 * Initilize and return the requested data structure with the given name or type if its supported
	 * 
	 * @param  name  name of the datastructure to initialize
	 * @param  type  implmentation type (KeyValueMap / KeyLongMap / DataObjectMap / FileWorkspaceMap)
	 * 
	 * @return initialized data structure if type is supported
	 */
	protected Core_DataStructure initDataStructure(String name, String type) {
		// Initialize for the respective type
		Core_DataStructure ret = null;
		if (type.equalsIgnoreCase("DataObjectMap")) {
			ret = new StructCache_DataObjectMap();
		} else if (type.equalsIgnoreCase("KeyValueMap")) {
			ret = new StructCache_KeyValueMap();
		}
		
		// If datastrucutre initialized, setup name
		if (ret != null) {
			ret.configMap().put("name", name);
			return ret;
		}
		
		// No valid type, return null
		return null;
	}
}
