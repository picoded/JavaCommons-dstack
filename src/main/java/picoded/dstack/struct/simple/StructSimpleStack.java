package picoded.dstack.struct.simple;

import picoded.core.struct.GenericConvertMap;
import picoded.dstack.core.*;
import picoded.dstack.struct.simple.*;
import picoded.dstack.*;

/**
 * [Internal use only]
 * 
 * StructSimple configuration based stack provider
 * 
	 * @return initialized data structure if type is supported
 **/
public class StructSimpleStack extends CoreStack {

	/**
	 * Constructor with configuration map
	 */
	public StructSimpleStack(GenericConvertMap<String,Object> inConfig) {
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
		if( type.equalsIgnoreCase("DataObjectMap") ) {
			return new StructSimple_DataObjectMap();
		} 
		if( type.equalsIgnoreCase("KeyValueMap") ) {
			return new StructSimple_KeyValueMap();
		} 
		if( type.equalsIgnoreCase("KeyLongMap") ) {
			return new StructSimple_KeyLongMap();
		} 
		if( type.equalsIgnoreCase("FileWorkspaceMap") ) {
			return new StructSimple_FileWorkspaceMap();
		}
		// No valid type, return null
		return null;
	}
}
