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
	
	/**
	 * @return keyLongMap of the given name, null if stack provider does not support the given object
	 */
	public KeyLongMap keyLongMap(String name);
	
	/**
	 * @return dataObjectMap of the given name, null if stack provider does not support the given object
	 */
	public DataObjectMap dataObjectMap(String name);
	
	/**
	 * @return fileWorkspaceMap of the given name, null if stack provider does not support the given object
	 */
	public FileWorkspaceMap fileWorkspaceMap(String name);
	
}