package picoded.dstack.jsql;

import picoded.core.conv.ConvertJSON;
import picoded.core.conv.GenericConvert;
import picoded.core.struct.GenericConvertHashMap;
import picoded.dstack.KeyValueMap;
import picoded.dstack.connector.jsql.JSql;
import picoded.dstack.struct.simple.StructSimpleStack_test;
import picoded.dstack.core.CoreStack;

public class JSqlStack_test extends StructSimpleStack_test {
	// To override for implementation
	//-----------------------------------------------------
	
	/// Note that this implementation constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public CoreStack implementationConstructor() {
		GenericConvertHashMap<String, Object> config = new GenericConvertHashMap<String, Object>();
		config.put("db", ConvertJSON.toMap("{ \"type\" : \"sqlite\" }"));
		return new JSqlStack(config);
	}
	
}
