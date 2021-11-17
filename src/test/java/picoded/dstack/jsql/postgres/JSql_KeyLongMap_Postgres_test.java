package picoded.dstack.jsql.postgres;

// Test depends
import picoded.dstack.*;
import picoded.dstack.jsql.*;
import picoded.dstack.connector.jsql.*;
import picoded.dstack.struct.simple.*;

// java imports
import java.security.SecureRandom;

public class JSql_KeyLongMap_Postgres_test extends JSql_KeyLongMap_test {
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Note that this SQL connector constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public JSql jsqlConnection() {
		return JSqlTestConnection.postgres();
	}
	
	// NOTE : Large long values (such as randomLong)
	//        is currently a known issue for mysql/postgres
	//        as it seems to clamp its accuracy to 20 digits (somehow)
	//        when its currently speced for 24 digits
	//        - This test is currently being clamped to int size in mysql/postgres
	@Test
	public void weakCompareAndSet_randomLong() {
		// Lets derive the "new" lock token - randomly!
		long testValue = Math.abs((new SecureRandom()).nextInt());
		testWeakCompareAndSetAccuracy(testValue);
	}
	
}
