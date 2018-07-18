package picoded.dstack.stack;

// Java dependencies
import java.util.*;

// External dependencies
import org.junit.*;
import static org.junit.Assert.*;

import picoded.core.conv.ConvertJSON;
// Project dependencies
import picoded.dstack.*;
import picoded.dstack.core.*;
import picoded.dstack.stack.*;

public class ProviderConfig_test {
	
	/**
	 * Test utility function used to build 
	 * a single provider config backend
	 * given its type
	 * 
	 * @param  type
	 * 
	 * @return  provider config map
	 */
	static public Map<String,Object> providerConfigTestMap(String name, String type) {
		Map<String,Object> ret = new HashMap<>();
		ret.put("name", name);
		ret.put("type", type);

		return ret;
	}

	/**
	 * Test the minimum implementation, a single struct simple
	 */
	@Test
	public void singleLayer_struct() {
		// The simplest config map possible
		List<Object> inConfigList = new ArrayList<Object>();
		inConfigList.add(providerConfigTestMap("local","StructSimple"));

		// Initialize provider config
		ProviderConfig provider = new ProviderConfig(inConfigList);

		// Get the provider stack
		CoreStack stack = provider.getProviderStack("local");
		assertNotNull(stack);
	}
}
