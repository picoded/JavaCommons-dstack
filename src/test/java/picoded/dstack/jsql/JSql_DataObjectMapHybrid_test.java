package picoded.dstack.jsql;

import org.junit.Test;
// Target test class
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

// Test Case include
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import picoded.core.conv.ConvertJSON;
import picoded.core.struct.GenericConvertHashMap;
import picoded.core.struct.GenericConvertMap;
// Test depends
import picoded.dstack.*;
import picoded.dstack.jsql.*;
import picoded.dstack.connector.jsql.*;
import picoded.dstack.struct.simple.*;

public class JSql_DataObjectMapHybrid_test {
	
	// To override for implementation
	//-----------------------------------------------------
	
	/// Note that this SQL connector constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public JSql jsqlConnection() {
		return JSqlTestConnection.mysql();
	}
    
    private DataObjectMap mtObj;
	
	/// Impomentation constructor for SQL
	public DataObjectMap implementationConstructor() {
        
		GenericConvertMap<String, Object> kycForm = new GenericConvertHashMap<>();
		GenericConvertMap<String, Object> fixedTableMap = new GenericConvertHashMap<>();
		Map<String, Object> relPep = new HashMap<>();
		Map<String, Object> oid = new HashMap<>();
		oid.put("name", "_OID");
		relPep.put("_oid", oid);
		relPep.put("KYC_PDF_OID", "VARCHAR(24)");
		relPep.put("FULL_NAME", "VARCHAR(256)");
		relPep.put("NRIC", "VARCHAR(32)");
		relPep.put("OCCUPATION", "VARCHAR(256)");
		relPep.put("NAME_OF_EMPLOYER", "VARCHAR(256)");
		fixedTableMap.put("REL_PEP", relPep);
		kycForm.put("fixedTableMap", fixedTableMap);
		return new JSql_DataObjectMap(jsqlConnection(), JSqlTestConfig.randomTablePrefix(), kycForm);
    }
    
    public void populateDatabase() {
        
    }
    
    @Before
    public void setup(){
		mtObj = implementationConstructor();
		mtObj.systemSetup();
    }
    
    @After
    public void destroy() {
		if (mtObj != null) {
			mtObj.systemDestroy();
		}
        mtObj = null;
    }
    
    @Test
    public void retrieveValidObjectShouldPass() {
        
		DataObject obj = mtObj.get("T27hNr92dzCcZWgw9pXBva");
		assertNotNull(obj);
		System.out.println(ConvertJSON.fromObject(obj));
    }
    
    @Test
    public void hybridTest() {
        
		DataObject[] objs= mtObj.query("FULL_NAME = ? ", new Object[]{"New Test Client"});
		
		System.out.println(ConvertJSON.fromObject(objs));
		assertNotNull(objs);
    }
    
    // @TODO: Delete, Put, Update, List
}