package picoded.dstack.file.simple;

import picoded.core.struct.GenericConvertMap;
import picoded.dstack.FileWorkspaceMap;
import picoded.dstack.core.CoreStack;
import picoded.dstack.core.Core_DataStructure;
import picoded.dstack.core.Core_FileWorkspaceMap;
import picoded.dstack.jsql.JSql_DataObjectMap;
import picoded.dstack.jsql.JSql_FileWorkspaceMap;
import picoded.dstack.jsql.JSql_KeyLongMap;
import picoded.dstack.jsql.JSql_KeyValueMap;
import picoded.dstack.connector.jsql.JSql;

public class FileSimpleStack extends CoreStack {
	
	protected String baseDir = "";
	
	/**
	 * Constructor with configuration map
	 */
	public FileSimpleStack(GenericConvertMap<String, Object> inConfig) {
		super(inConfig);
		
		// Extract the connection config object
		String storage = inConfig.fetchString("storage");
		
		// If DB config is missing, throw
		if (storage == null) {
			throw new IllegalArgumentException("Missing 'storage' setting for file workspace.");
		}
		
		baseDir = storage;
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
		
		//	appendNameSpace is a configuration setting in cases where developers
		//	want to append the regex value of the namespace as part of the directory
		//	this attribute can be set as true to achieve that.
		//	"provider" : [
		//		{
		//			"name" : "project_workspace",
		//			"type" : "filelayered",
		//			"storage" : "./storage/webstudio",
		//			"appendNameSpace": true <-- set as true/false accordingly
		//		}
		//	],
		//	namespace" : [
		//		{
		//			"regex" : "PROJECTWORKSPACE", <-- this value to be append as part of the directory
		//			"providers" : [ "project_workspace" ]
		//		}
		//	]
		//
		boolean appendNameSpace = this.config.getBoolean("appendNameSpace", true);
		
		if (type.equalsIgnoreCase("FileWorkspaceMap")) {
			String fileNamespace = baseDir;
			if (appendNameSpace) {
				fileNamespace = baseDir + "/" + name;
			}
			return new FileSimple_FileWorkspaceMap(fileNamespace);
		}
		// No valid type, return null
		return null;
	}
	
	/**
	 * @return fileWorkspaceMap of the given name, null if stack provider does not support the given object
	 */
	@Override
	public FileWorkspaceMap fileWorkspaceMap(String name) {
		return (FileSimple_FileWorkspaceMap) cacheDataStructure(name, "FileWorkspaceMap",
			FileSimple_FileWorkspaceMap.class);
	}
}
