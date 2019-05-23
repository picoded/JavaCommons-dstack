package picoded.dstack.jsql;

import java.net.ServerSocket;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.RandomStringUtils;

///
/// The SQL configuration vars, this is to ensure a centralized place to change the various test configuration values
/// Value access were made as functions, to facilitate future migration to config files??
///
public class JSqlTestConfig {
	
	///
	/// Randomly generated table prefix, used to prevent multiple running tests from colliding
	///
	static public String randomTablePrefix() {
		return "T" + RandomStringUtils.randomAlphanumeric(7).toUpperCase();
	}
	
	//
	// Issue a somewhat unique port number for use in test cases
	//
	public synchronized static int issuePortNumber() {
		// Start with a random port between 10k to 50k
		int portCounter = ThreadLocalRandom.current().nextInt(10000, 50000);
		
		// Increment if a conflict is found
		int checkTries = 0;
		while (isLocalPortInUse(portCounter)) {
			portCounter = ThreadLocalRandom.current().nextInt(10000, 50000);
			
			++checkTries;
			if (checkTries > 1000) {
				throw new RuntimeException("Attempted over " + checkTries
					+ " to get a local port =( sad");
			}
		}
		
		// Returns the port counter
		return portCounter;
	}
	
	/// Utility function used to test if a localhost port is in use, if so skip its "issue process"
	/// 
	/// @param   Port number to test
	///
	/// @return  true if its in use
	private static boolean isLocalPortInUse(int port) {
		try {
			// ServerSocket try to open a LOCAL port
			new ServerSocket(port).close();
			// local port can be opened, it's available
			return false;
		} catch (Exception e) {
			// local port cannot be opened, it's in use
			return true;
		}
	}
	
	//-------------------------------//
	// Default Credentials for MYSQL //
	//-------------------------------//
	static private String MYSQL_HOST = "demodb-mysql.picoded-dev.com";
	static private String MYSQL_DATA = "JAVACOMMONS";
	static private String MYSQL_USER = "JAVACOMMONS";
	static private String MYSQL_PASS = "JAVACOMMONS";
	static private int MYSQL_PORT = 3306;
	
	static public String MYSQL_HOST() {
		return MYSQL_HOST;
	}
	
	static public int MYSQL_PORT() {
		return MYSQL_PORT;
	}
	
	static public String MYSQL_CONN() {
		return MYSQL_HOST + ":" + MYSQL_PORT;
	}
	
	static public String MYSQL_DATA() {
		return MYSQL_DATA;
	}
	
	static public String MYSQL_USER() {
		return MYSQL_USER;
	}
	
	static public String MYSQL_PASS() {
		return MYSQL_PASS;
	}
	
	static public String MYSQL_CONN_JDBC() {
		return "jdbc:mysql://" + MYSQL_CONN() + "/" + MYSQL_DATA;
	}
	
	static public Properties MYSQL_CONN_PROPS() {
		Properties connectionProps = new Properties();
		connectionProps.put("user", "JAVACOMMONS");
		connectionProps.put("password", "JAVACOMMONS");
		connectionProps.put("autoReconnect", "true");
		connectionProps.put("failOverReadOnly", "false");
		connectionProps.put("maxReconnects", "5");
		return connectionProps;
	}
	
	//-------------------------------//
	// Default Credentials for MSSQL //
	//-------------------------------//
	static private String MSSQL_HOST = "demodb-mssql.picoded-dev.com";
	static private String MSSQL_NAME = "JAVACOMMONS";
	static private String MSSQL_USER = "JAVACOMMONS";
	static private String MSSQL_PASS = "JAVACOMMONS";
	static private int MSSQL_PORT = 1433;
	
	static public String MSSQL_HOST() {
		return MSSQL_HOST;
	}
	
	static public int MSSQL_PORT() {
		return MSSQL_PORT;
	}
	
	static public String MSSQL_CONN() {
		return MSSQL_HOST; // +":"+MSSQL_PORT;
	}
	
	static public String MSSQL_NAME() {
		return MSSQL_NAME;
	}
	
	static public String MSSQL_USER() {
		return MSSQL_USER;
	}
	
	static public String MSSQL_PASS() {
		return MSSQL_PASS;
	}
	
	//--------------------------------//
	// Default Credentials for ORACLE //
	//--------------------------------//
	static private String ORACLE_PATH = "@//salesbox-db-oracle.cvbukxarewjf.ap-southeast-1.rds.amazonaws.com:1521/ORCL";
	static private String ORACLE_USER = "JAVACOMMONS";
	static private String ORACLE_PASS = "JAVACOMMONS";
	
	static public String ORACLE_PATH() {
		return ORACLE_PATH;
	}
	
	static public String ORACLE_USER() {
		return ORACLE_USER;
	}
	
	static public String ORACLE_PASS() {
		return ORACLE_PASS;
	}
	
	//----------------------------------//
	// Default Credentials for POSTGRES //
	//----------------------------------//
	static private String POSTGRES_HOST = "127.0.0.1";
	static private String POSTGRES_NAME = "JAVACOMMONS";
	static private String POSTGRES_USER = "JAVACOMMONS";
	static private String POSTGRES_PASS = "JAVACOMMONS";
	static private int POSTGRES_PORT = 5432;
	
	static public String POSTGRES_HOST() {
		return POSTGRES_HOST;
	}
	
	static public String POSTGRES_NAME() {
		return POSTGRES_NAME;
	}
	
	static public String POSTGRES_USER() {
		return POSTGRES_USER;
	}
	
	static public String POSTGRES_PASS() {
		return POSTGRES_PASS;
	}
	
	static public int POSTGRES_PORT() {
		return POSTGRES_PORT;
	}
	
	//-------------------------------//
	// LDAP Server location          //
	//-------------------------------//
	static private String LDAP_HOST = "demodb-ldap.picoded-dev.com";
	static private String LDAP_DOMAIN = "com.demo";
	static private int LDAP_PORT = 389;
	
	static public String LDAP_HOST() {
		return LDAP_HOST;
	}
	
	static public String LDAP_DOMAIN() {
		return LDAP_DOMAIN;
	}
	
	static public int LDAP_PORT() {
		return LDAP_PORT;
	}
	
}
