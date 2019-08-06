package picoded.dstack;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import picoded.core.common.SystemSetupInterface;
import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.template.AbstractSystemSetupInterfaceCollection;
import picoded.dstack.*;

/**
 * Stack common interface
 **/
public interface CommonStack extends AbstractSystemSetupInterfaceCollection {
	
	/**
	 * @return keyValueMap of the given name, null if stack provider does not support the given object
	 */
	public KeyValueMap keyValueMap(String name);
	
	public default KeyValueMap getKeyValueMap(String name) {
		return keyValueMap(name);
	}
	
	/**
	 * @return keyLongMap of the given name, null if stack provider does not support the given object
	 */
	public KeyLongMap keyLongMap(String name);
	
	public default KeyLongMap getKeyLongMap(String name) {
		return keyLongMap(name);
	}
	
	/**
	 * @return dataObjectMap of the given name, null if stack provider does not support the given object
	 */
	public DataObjectMap dataObjectMap(String name);
	
	public default DataObjectMap getDataObjectMap(String name) {
		return dataObjectMap(name);
	}
	
	/**
	 * @return fileWorkspaceMap of the given name, null if stack provider does not support the given object
	 */
	public FileWorkspaceMap fileWorkspaceMap(String name);
	
	public default FileWorkspaceMap getFileWorkspaceMap(String name) {
		return fileWorkspaceMap(name);
	}
	
}