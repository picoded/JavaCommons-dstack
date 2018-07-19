package picoded.dstack;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.management.RuntimeErrorException;

import picoded.core.conv.ArrayConv;
import picoded.core.conv.GenericConvert;
import picoded.core.struct.GenericConvertHashMap;
import picoded.core.struct.GenericConvertList;
import picoded.core.struct.GenericConvertMap;
import picoded.dstack.core.CoreStack;
import picoded.dstack.core.Core_DataStructure;
import picoded.dstack.core.Core_FileWorkspaceMap;
import picoded.dstack.core.Core_KeyLong;
import picoded.dstack.core.Core_KeyLongMap;
import picoded.dstack.core.Core_KeyValueMap;
import picoded.dstack.core.Core_DataObjectMap;
import picoded.dstack.stack.ProviderConfig;
import picoded.dstack.stack.Stack_DataObjectMap;
import picoded.dstack.stack.Stack_FileWorkspaceMap;
import picoded.dstack.stack.Stack_KeyLongMap;
import picoded.dstack.stack.Stack_KeyValueMap;

public class DStack extends CoreStack {
	
	protected ProviderConfig providerConfig;

	protected GenericConvertList<Object> namespace;
	protected GenericConvertList<Object> providerConfigList;

	/**
	 * Constructor with configuration map
	 */
	public DStack(GenericConvertMap<String, Object> inConfig) {
		super(inConfig);

		providerConfigList = inConfig.fetchGenericConvertList("provider");
		if(providerConfigList == null) {
			throw new IllegalArgumentException("Missing `provider` config list");
		}

		providerConfig = new ProviderConfig(providerConfigList);
		namespace = inConfig.fetchGenericConvertList("namespace");

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

		// Grab the first match namepsace configuration
		GenericConvertMap<String,Object> namespaceConfig = resolveNamespaceConfig(name);
		if ( namespaceConfig == null ) {
			throw new RuntimeException("No namespace configuration found for " + name);
		}

		// Obtain the regex and the providers from the name space to search through
		String regex = namespaceConfig.getString("regex");
		List<Object> providerList = namespaceConfig.getObjectList("providers");
		if ( providerList == null ) {
			throw new RuntimeException("No `providers` found in namespaceConfig of "+regex);
		}

		// Initialize for the respective type
		if (type.equalsIgnoreCase("DataObjectMap")) {
			return returnStackDataObjectMap(providerList, name, regex);
		}
		if (type.equalsIgnoreCase("KeyValueMap")) {
			return returnStackKeyValueMap(providerList, name, regex);
		}
		if (type.equalsIgnoreCase("KeyLongMap")) {
			return returnStackKeyLongMap(providerList, name, regex);
		}
		if (type.equalsIgnoreCase("FileWorkspaceMap")) {
			return returnStackFileWorkspaceMap(providerList, name, regex);
		}

		return null;
	}

	//
	// 
	//

	protected Stack_DataObjectMap returnStackDataObjectMap(List<Object> providersList, String name, String regex){
		List<Core_DataObjectMap> dataObjectMapList = retrieveDataObjectMapList(providersList, name, regex);

		Core_DataObjectMap[] dataObjectMapArray = dataObjectMapList.toArray(new Core_DataObjectMap[dataObjectMapList.size()]);

		Stack_DataObjectMap ret = new Stack_DataObjectMap( dataObjectMapArray );

		return ret;
	}

	protected Stack_KeyValueMap returnStackKeyValueMap(List<Object> providersList, String name, String regex){
		List<Core_KeyValueMap> keyValueMapList = retrieveKeyValueMapList(providersList, name, regex);

		Core_KeyValueMap[] keyValueMapArray = keyValueMapList.toArray(new Core_KeyValueMap[keyValueMapList.size()]);

		Stack_KeyValueMap ret = new Stack_KeyValueMap( keyValueMapArray );

		return ret;
	}

	protected Stack_KeyLongMap returnStackKeyLongMap(List<Object> providersList, String name, String regex){
		List<Core_KeyLongMap> keyLongMapList = retrieveKeyLongMapList(providersList, name, regex);

		Core_KeyLongMap[] keyLongMapArray = keyLongMapList.toArray(new Core_KeyLongMap[keyLongMapList.size()]);

		Stack_KeyLongMap ret = new Stack_KeyLongMap( keyLongMapArray );

		return ret;
	}

	protected Stack_FileWorkspaceMap returnStackFileWorkspaceMap(List<Object> providersList, String name, String regex){
		List<Core_FileWorkspaceMap> fileWorkspaceMapList = retrieveFileWorkspaceMapList(providersList, name, regex);

		Core_FileWorkspaceMap[] fileWorkspaceMapArray = fileWorkspaceMapList.toArray(new Core_FileWorkspaceMap[fileWorkspaceMapList.size()]);

		Stack_FileWorkspaceMap ret = new Stack_FileWorkspaceMap( fileWorkspaceMapArray );

		return ret;
	}

	protected List<Core_DataObjectMap> retrieveDataObjectMapList(List<Object> providersList, String name, String regex){
		List<Core_DataObjectMap> stackDataObjectMapList = new ArrayList<>();
		for ( Object object : providersList ) {
			String nameOfProvider = "";
			if ( object instanceof String ) {
				 nameOfProvider = object.toString();
			}

			Core_DataObjectMap dataObjectMap = providerConfig.getProviderStack(nameOfProvider).dataObjectMap(name);
			if ( dataObjectMap == null ) {
				throw new RuntimeException(nameOfProvider +" in `providers` of `namespace` under the `regex`"+regex+" cannot be found.");
			}
			stackDataObjectMapList.add(dataObjectMap);
		}
		return stackDataObjectMapList;
	}

	protected List<Core_KeyValueMap> retrieveKeyValueMapList(List<Object> providersList, String name, String regex){
		List<Core_KeyValueMap> stackKeyValueMapList = new ArrayList<>();
		for ( Object object : providersList ) {
			String nameOfProvider = "";
			if ( object instanceof String ) {
				 nameOfProvider = object.toString();
			}

			Core_KeyValueMap keyValueMap = providerConfig.getProviderStack(nameOfProvider).keyValueMap(name);
			if ( keyValueMap == null ) {
				throw new RuntimeException(nameOfProvider +" in `providers` of `namespace` under the `regex`"+regex+" cannot be found.");
			}
			stackKeyValueMapList.add(keyValueMap);
		}
		return stackKeyValueMapList;
	}

	protected List<Core_KeyLongMap> retrieveKeyLongMapList(List<Object> providersList, String name, String regex){
		List<Core_KeyLongMap> stackKeyLongMapList = new ArrayList<>();
		for ( Object object : providersList ) {
			String nameOfProvider = "";
			if ( object instanceof String ) {
				 nameOfProvider = object.toString();
			}

			Core_KeyLongMap keyLongMap = providerConfig.getProviderStack(nameOfProvider).keyLongMap(name);
			if ( keyLongMap == null ) {
				throw new RuntimeException(nameOfProvider +" in `providers` of `namespace` under the `regex`"+regex+" cannot be found.");
			}
			stackKeyLongMapList.add(keyLongMap);
		}
		return stackKeyLongMapList;
	}

	protected List<Core_FileWorkspaceMap> retrieveFileWorkspaceMapList(List<Object> providersList, String name, String regex){
		List<Core_FileWorkspaceMap> stackFileWorkspaceMapList = new ArrayList<>();
		for ( Object object : providersList ) {
			String nameOfProvider = "";
			if ( object instanceof String ) {
				 nameOfProvider = object.toString();
			}

			Core_FileWorkspaceMap fileWorkspaceMap = providerConfig.getProviderStack(nameOfProvider).fileWorkspaceMap(name);
			if ( fileWorkspaceMap == null ) {
				throw new RuntimeException(nameOfProvider +" in `providers` of `namespace` under the `regex`"+regex+" cannot be found.");
			}
			stackFileWorkspaceMapList.add(fileWorkspaceMap);
		}
		return stackFileWorkspaceMapList;
	}


	//
	// Helper Functions
	//

	/**
	 * This function will find the first namespaceConfig that matches the requested name
	 * 
	 * @param name of the object to be searched
	 * 
	 * @return the configuration or null
	 */
	protected GenericConvertMap<String, Object> resolveNamespaceConfig(String name) {
		for(Object object : namespace){
			GenericConvertMap<String, Object> namespaceConfig = GenericConvert.toGenericConvertStringMap(object);
			String pattern = namespaceConfig.getString("regex", "");
			if(regexNameMatcher(name, pattern)){
				return namespaceConfig;
			}
		}
		return null;
	}

	/**
	 * Attempts to match the name with the regex pattern given in the param
	 * 
	 * @param nameToMatch is the string to check if it matches with the pattern
	 * @param pattern     is the `regex` that is set in the namespace 
	 * 
	 * @return 
	 */
	protected boolean regexNameMatcher(String nameToMatch, String pattern) {
		return nameToMatch.matches(pattern);
	}

}