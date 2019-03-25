package picoded.dstack;

import java.util.ArrayList;
import java.util.HashMap;

import picoded.core.conv.ConvertJSON;
import picoded.core.conv.GenericConvert;
import picoded.core.struct.GenericConvertHashMap;
import picoded.dstack.KeyValueMap;
import picoded.dstack.connector.jsql.JSql;
import picoded.dstack.struct.simple.StructSimpleStack_test;
import picoded.dstack.core.CoreStack;

public class DStack_test extends StructSimpleStack_test {
	// To override for implementation
	//-----------------------------------------------------
	
	/// Note that this implementation constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public CoreStack implementationConstructor() {
		GenericConvertHashMap<String, Object> config = new GenericConvertHashMap<String, Object>();
		ArrayList<Object> providers = new ArrayList();
		
		HashMap<String, Object> provider = new HashMap<>();
		provider.put("name", "db_main");
		
		provider.put("type", "StructSimple");
		
		providers.add(provider);
		
		config.put("provider", providers);
		
		ArrayList<Object> namespaces = new ArrayList<>();
		HashMap<String, Object> namespace = new HashMap<>();
		
		ArrayList<String> namespace_providers = new ArrayList<>();
		namespace_providers.add("db_main");
		namespace.put("regex", ".*");
		namespace.put("providers", namespace_providers);
		
		namespaces.add(namespace);
		
		config.put("namespace", namespaces);
		return new DStack(config);
	}
	
}
