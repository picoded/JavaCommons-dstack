package picoded.dstack.connector.jsql;

import java.io.File;

import com.zaxxer.hikari.*;
import picoded.core.struct.*;

/**
 * HikariCP utility class
 **/
class HikaricpUtil {
	
	//----------------------------------------------------------------------------------
	//
	// Configuration loading from map
	//
	//----------------------------------------------------------------------------------
	
	/**
	 * Loading the common HikariConfig settings - given a config map.
	 *
	 * This exclude database specific configuration loading
	 *
	 * @param  config map used
	 */
	public static HikariConfig commonConfigLoading(GenericConvertMap config) {
		
		// Fallback to a blank map (if needed)
		if (config == null) {
			config = new GenericConvertHashMap<>();
		}
		
		// config object
		HikariConfig ret = new HikariConfig();
		
		//
		// Setup the various individual config, with default fallback
		//
		// For additional config to add support in future (if needed) see
		// https://github.com/brettwooldridge/HikariCP
		//
		
		// autoCommit
		ret.setAutoCommit(config.getBoolean("autoCommit", ret.isAutoCommit()));
		
		// connectionTimeout
		ret.setConnectionTimeout(config.getLong("connectionTimeout", ret.getConnectionTimeout()));
		
		// idleTimeout
		System.out.println("SETIDLETIMEOUT : " + config.getLong("idleTimeout", ret.getIdleTimeout()));
		ret.setIdleTimeout(config.getLong("idleTimeout", ret.getIdleTimeout()));
		
		// maxLifetime
		System.out.println("SETMAXLIFETIME : " + config.getLong("maxLifetime", ret.getMaxLifetime()));
		ret.setMaxLifetime(config.getLong("maxLifetime", ret.getMaxLifetime()));
		
		// connectionTestQuery
		// **not supported:** we enforce JDBC4 and above drivers (for now)
		
		// maximumPoolSize
		System.out.println("CONFIG POOL SIZE : " + config.getInt("maximumPoolSize"));
		System.out.println("DEFAULT POOL SIZE : " + defaultMaxPoolSize());
		System.out.println("MAX POOL SIZE : "
			+ config.getInt("maximumPoolSize", defaultMaxPoolSize()));
		ret.setMaximumPoolSize(config.getInt("maximumPoolSize", defaultMaxPoolSize()));
		// ret.setMaximumPoolSize(10);
		
		// minimumIdle
		ret.setMinimumIdle(config.getInt("minimumIdle", 2));
		
		// Return config object
		return ret;
	}
	
	//----------------------------------------------------------------------------------
	//
	// Utility functionality
	//
	//----------------------------------------------------------------------------------
	
	//
	// Default configuration handling
	//
	
	// static memoizer for the default max pool size
	static int _defaultMaxPoolSize = -1;
	
	/**
	 * Default max pool size is always atleast 4
	 *
	 * Default max pool size will be tagged to the total avaliableProcessors count.
	 * Which would 2 * number of cores, for a hyper threading environment.
	 *
	 * This is close to the optimal guideline for max coonection pool sizing
	 * See : https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing
	 *
	 * If hyperthreading is disabled, this estimate is ineffective.
	 * But is considered out of scope.
	 *
	 * @return default max pool size to use
	 */
	public static int defaultMaxPoolSize() {
		// Current pool size has not been calculated - calculate it
		if (_defaultMaxPoolSize <= 0) {
			_defaultMaxPoolSize = Math.max(2, Runtime.getRuntime().availableProcessors());
		}
		return _defaultMaxPoolSize;
	}
	
	//----------------------------------------------------------------------------------
	//
	// SQL specific setup
	//
	//----------------------------------------------------------------------------------
	
	/**
	 * Loads a HikariDataSource for SQLite given the config
	 *
	 * @param  config map used
	 *
	 * @return HikariDataSource with the appropriate config loaded and initialized
	 */
	public static HikariDataSource sqlite(GenericConvertMap config) {
		// Lets get the sqlite path
		String path = config.getString("path", ":memory:");
		if (path == null || path.length() == 0) {
			throw new RuntimeException("Missing path configuration for SQLite connection");
		}
		
		// Get the absolute file path to use for sqlite
		String absolutePath = null;
		
		// In memory mode uses :memory: respectively
		if (path.equalsIgnoreCase(":memory:")) {
			absolutePath = ":memory:";
		} else {
			// Get the sqlite file
			File sqliteFileObj = new File(path);
			absolutePath = sqliteFileObj.getAbsolutePath();
			
			// And check if the path is a directory
			// if so it throws an error as the file is not the following:
			// - a non existing file (which sqlite will initialize)
			// - a file (which sqlite will read from)
			if (sqliteFileObj.isDirectory()) {
				throw new RuntimeException(
					"Invalid file path found for sqlite - found a directory instead : " + absolutePath);
			}
		}
		
		// Load the common config
		HikariConfig hconfig = commonConfigLoading(config);
		
		// Load the DB library
		// This is only imported on demand, avoid preloading until needed
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(
				"Failed to load SQLite JDBC driver - please ensure 'org.sqlite.JDBC' jar is included");
		}
		
		// Setup the configured
		hconfig.setDriverClassName("org.sqlite.JDBC");
		hconfig.setJdbcUrl("jdbc:sqlite:" + absolutePath);
		
		// Initialize the data source
		return new HikariDataSource(hconfig);
	}
	
	/**
	 * Loads a HikariDataSource for Mysql given the config
	 *
	 * @param  config map used
	 *
	 * @return HikariDataSource with the appropriate config loaded and initialized
	 */
	public static HikariDataSource mysql(GenericConvertMap config) {
		// Lets get the mysql required parameters
		String host = config.getString("host", "localhost");
		int port = config.getInt("port", 3306);
		String name = config.getString("name", null);
		String user = config.getString("user", null);
		String pass = config.getString("pass", null);
		
		// Perform simple validation of mysql params
		if (host == null || host.length() == 0) {
			throw new RuntimeException("Missing host configuration for MYSql connection");
		}
		if (name == null || name.length() == 0) {
			throw new RuntimeException("Missing name configuration for MYSql connection");
		}
		if (user == null || user.length() == 0) {
			throw new RuntimeException("Missing user configuration for MYSql connection");
		}
		if (pass == null || pass.length() == 0) {
			throw new RuntimeException("Missing pass configuration for MYSql connection");
		}
		
		// Load the common config
		HikariConfig hconfig = commonConfigLoading(config);
		
		// Load the DB library
		// This is only imported on demand, avoid preloading until needed
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(
				"Failed to load MySQL JDBC driver - please ensure 'com.mysql.JDBC' jar is included");
		}
		
		// Setup the configured connection URL + DB
		hconfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
		
		hconfig.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + name);
		
		// Setup the username and password
		hconfig.setUsername(user);
		hconfig.setPassword(pass);
		
		//
		// Setup the datasource config
		// See: https://github.com/brettwooldridge/HikariCP/wiki/MySQL-Configuration
		//
		hconfig.addDataSourceProperty("cachePrepStmts", //
			config.getBoolean("cachePrepStmts", true) //
			);
		hconfig.addDataSourceProperty("prepStmtCacheSize", //
			config.getInt("prepStmtCacheSize", 250) //
			);
		hconfig.addDataSourceProperty("prepStmtCacheSqlLimit", //
			config.getInt("prepStmtCacheSqlLimit", 2048) //
			);
		hconfig.addDataSourceProperty("useServerPrepStmts", //
			config.getBoolean("useServerPrepStmts", true) //
			);
		hconfig.addDataSourceProperty("useLocalSessionState", //
			config.getBoolean("useLocalSessionState", true) //
			);
		hconfig.addDataSourceProperty("rewriteBatchedStatements", //
			config.getBoolean("rewriteBatchedStatements", true) //
			);
		hconfig.addDataSourceProperty("cacheResultSetMetadata", //
			config.getBoolean("cacheResultSetMetadata", true) //
			);
		hconfig.addDataSourceProperty("cacheServerConfiguration", //
			config.getBoolean("cacheServerConfiguration", true) //
			);
		hconfig.addDataSourceProperty("elideSetAutoCommits", //
			config.getBoolean("elideSetAutoCommits", true) //
			);
		hconfig.addDataSourceProperty("maintainTimeStats", //
			config.getBoolean("maintainTimeStats", false) //
			);
		
		// Initialize the data source and return
		return new HikariDataSource(hconfig);
	}
	
	/**
	 * Loads a HikariDataSource for Mysql given the config
	 *
	 * @param  config map used
	 *
	 * @return HikariDataSource with the appropriate config loaded and initialized
	 */
	public static HikariDataSource oracle(GenericConvertMap config) {
		// Lets get the mysql required parameters
		String host = config.getString("host", "localhost");
		int port = config.getInt("port", 1521);
		String user = config.getString("user", null);
		String pass = config.getString("pass", null);
		
		// "type" : "oracle",
		// "path" : "@//salesbox-db-oracle.cvbukxarewjf.ap-southeast-1.rds.amazonaws.com:1521/ORCL",
		// "user" : "root",
		// "pass" : "Rv-W54ytUmMyWy9k_gg7dL",
		
		// Perform simple validation of mysql params
		if (host == null || host.length() == 0) {
			throw new RuntimeException("Missing host configuration for Oracle connection");
		}
		if (user == null || user.length() == 0) {
			throw new RuntimeException("Missing user configuration for Oracle connection");
		}
		if (pass == null || pass.length() == 0) {
			throw new RuntimeException("Missing pass configuration for Oracle connection");
		}
		
		// Load the common config
		HikariConfig hconfig = commonConfigLoading(config);
		
		// Load the DB library
		// This is only imported on demand, avoid preloading until needed
		// try {
		// 	Class.forName("com.mysql.cj.jdbc.Driver");
		// } catch (ClassNotFoundException e) {
		// 	throw new RuntimeException(
		// 		"Failed to load MySQL JDBC driver - please ensure 'com.mysql.JDBC' jar is included");
		// }
		
		// Setup the configured connection URL + DB
		hconfig.setDriverClassName("oracle.jdbc.pool.OracleDataSource");
		
		// hconfig.setJdbcUrl("jdbc:oracle:thin:" + host + ":" + port + "/" + name);
		hconfig.setJdbcUrl("jdbc:oracle:thin:" + host);
		
		System.out.println("HIKARI JDBC URL:: jdbc:oracle:thin:" + host);
		
		// Setup the username and password
		hconfig.setUsername(user);
		hconfig.setPassword(pass);
		
		hconfig.setConnectionTimeout(10000);
		hconfig.setIdleTimeout(60000);
		hconfig.setMaxLifetime(60000);
		
		// hconfig.setLeakDetectionThreshold(30000);
		
		//
		// Setup the datasource config
		// See: https://github.com/brettwooldridge/HikariCP/wiki/MySQL-Configuration
		//
		// hconfig.addDataSourceProperty("cachePrepStmts", //
		// 	config.getBoolean("cachePrepStmts", true) //
		// 	);
		// hconfig.addDataSourceProperty("prepStmtCacheSize", //
		// 	config.getInt("prepStmtCacheSize", 250) //
		// 	);
		// hconfig.addDataSourceProperty("prepStmtCacheSqlLimit", //
		// 	config.getInt("prepStmtCacheSqlLimit", 2048) //
		// 	);
		// hconfig.addDataSourceProperty("useServerPrepStmts", //
		// 	config.getBoolean("useServerPrepStmts", true) //
		// 	);
		// hconfig.addDataSourceProperty("useLocalSessionState", //
		// 	config.getBoolean("useLocalSessionState", true) //
		// 	);
		// hconfig.addDataSourceProperty("rewriteBatchedStatements", //
		// 	config.getBoolean("rewriteBatchedStatements", true) //
		// 	);
		// hconfig.addDataSourceProperty("cacheResultSetMetadata", //
		// 	config.getBoolean("cacheResultSetMetadata", true) //
		// 	);
		// hconfig.addDataSourceProperty("cacheServerConfiguration", //
		// 	config.getBoolean("cacheServerConfiguration", true) //
		// 	);
		// hconfig.addDataSourceProperty("elideSetAutoCommits", //
		// 	config.getBoolean("elideSetAutoCommits", true) //
		// 	);
		// hconfig.addDataSourceProperty("maintainTimeStats", //
		// 	config.getBoolean("maintainTimeStats", false) //
		// 	);
		
		// Initialize the data source and return
		return new HikariDataSource(hconfig);
	}
	
	/**
	 * Loads a HikariDataSource for MS-SQL given the config
	 *
	 * @param  config map used
	 *
	 * @return HikariDataSource with the appropriate config loaded and initialized
	 */
	public static HikariDataSource mssql(GenericConvertMap config) {
		// Lets get the MSSql required parameters
		String host = config.getString("host", "localhost");
		int port = config.getInt("port", 1433);
		String name = config.getString("name", null);
		String user = config.getString("user", null);
		String pass = config.getString("pass", null);
		
		// Perform simple validation of MSSql params
		if (host == null || host.length() == 0) {
			throw new RuntimeException("Missing host configuration for MSSql connection");
		}
		if (name == null || name.length() == 0) {
			throw new RuntimeException("Missing name configuration for MSSql connection");
		}
		if (user == null || user.length() == 0) {
			throw new RuntimeException("Missing user configuration for MSSql connection");
		}
		if (pass == null || pass.length() == 0) {
			throw new RuntimeException("Missing pass configuration for MSSql connection");
		}
		
		// Load the common config
		HikariConfig hconfig = commonConfigLoading(config);
		
		// Load the DB library
		// This is only imported on demand, avoid preloading until needed
		// try {
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDataSource");
		
		// // manually import of classes
		// Class.forName("com.microsoft.sqlserver.jdbc.InputStreamGetterArgs");
		// Class.forName("com.microsoft.sqlserver.jdbc.SSLenType");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection$State");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSChannel$SSLHandhsakeState");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection");
		// Class.forName("com.microsoft.sqlserver.jdbc.Column");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$10");
		// Class
		// 	.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$DrainStatus");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.StreamColInfo");
		// Class.forName("com.microsoft.sqlserver.jdbc.JavaType$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.DDC");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResultSetMetaData");
		// Class.forName("com.microsoft.sqlserver.jdbc.JDBCType");
		// Class.forName("com.microsoft.sqlserver.jdbc.KerbAuthentication$2");
		// Class.forName("com.microsoft.sqlserver.jdbc.StreamPacket");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerPooledConnection");
		// Class.forName("com.microsoft.sqlserver.jdbc.KerbCallback");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDataSource");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerCallableStatement");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement$PrepStmtExecCmd");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$BigOrSmallByteLenStrategy");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerException");
		// Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerSavepoint");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerParameterMetaData");
		// Class.forName("com.microsoft.sqlserver.jdbc.DescribeParameterEncryptionResultSet1");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.Weighers$IterableWeigher");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerLob");
		// Class
		// 	.forName("mssql.googlecode.concurrentlinkedhashmap.LinkedDeque$AbstractLinkedIterator");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$5");
		// Class.forName("com.microsoft.sqlserver.jdbc.Nanos");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResource_it");
		// Class
		// 	.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$UpdateTask");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection$1ConnectionCommand");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$KeySet");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerBulkCopy$ColumnMapping");
		// Class.forName("com.microsoft.sqlserver.jdbc.DriverError");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.Weighers$MapWeigher");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$16");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$15");
		// Class.forName("com.microsoft.sqlserver.jdbc.StreamInfo");
		// Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerStatement");
		// Class.forName("com.microsoft.sqlserver.jdbc.KerbAuthentication$3");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$AddTask");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResource_fr");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection$FedAuthTokenCommand");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SSPIAuthentication");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLCollation");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerEncryptionAlgorithmFactoryList");
		
		// Class.forName("microsoft.sql.DateTimeOffset$SerializationProxy");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.Weighers$ListWeigher");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.EntryWeigher");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.Linked");
		// Class.forName("com.microsoft.sqlserver.jdbc.ColumnEncryptionSetting");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSChannel$ProxyOutputStream");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerBulkCopy$BulkTimeoutTimer");
		// Class.forName("com.microsoft.sqlserver.jdbc.RowType");
		// Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerConnection");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriverBooleanProperty");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$FixedLenStrategy");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerAeadAes256CbcHmac256EncryptionKey");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection$1DTCCommand");
		// Class.forName("com.microsoft.sqlserver.jdbc.DTVImpl");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDataTable$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.Shape");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		// Class
		// 	.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$WriteThroughEntry");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResultSet$1ClientCursorInitializer");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerKeyVaultAuthenticationCallback");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerStatement$StmtBatchExecCmd");
		// Class.forName("com.microsoft.sqlserver.jdbc.FailoverInfo");
		// Class.forName("com.microsoft.sqlserver.jdbc.StreamError");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResultSet$1UpdateRowRPC");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSChannel");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerBulkCopy$BulkColumnMetaData");
		// Class.forName("com.microsoft.sqlserver.jdbc.Util$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.SocketFinder");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerStatement$ExecuteProperties");
		// Class.forName("com.microsoft.sqlserver.jdbc.InternalSpatialDatatype");
		// Class.forName("com.microsoft.sqlserver.jdbc.TimeoutTimer");
		// Class.forName("com.microsoft.sqlserver.jdbc.ServerPortPlaceHolder");
		// Class.forName("com.microsoft.sqlserver.jdbc.Encoding");
		// Class
		// 	.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$KatmaiScaledTemporalStrategy");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.dataclassification.Label");
		// Class.forName("com.microsoft.sqlserver.jdbc.CekTable");
		// Class.forName("com.microsoft.sqlserver.jdbc.dns.DNSUtilities");
		// Class.forName("com.microsoft.sqlserver.jdbc.DriverJDBCVersion");
		// Class.forName("com.microsoft.sqlserver.jdbc.ServerDTVImpl");
		// Class.forName("com.microsoft.sqlserver.jdbc.AsciiFilteredInputStream");
		// Class.forName("com.microsoft.sqlserver.jdbc.DataTypes");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$7");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSReader");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLIdentifier");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSChannel$SSLHandshakeOutputStream");
		// Class.forName("com.microsoft.sqlserver.jdbc.SqlFedAuthToken");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResource");
		// Class.forName("com.microsoft.sqlserver.jdbc.AuthenticationScheme");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSWriter$1");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.Weighers$ByteArrayWeigher");
		// Class.forName("com.microsoft.sqlserver.jdbc.UserTypes");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSWriter");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$4");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResource_pt_BR");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerSQLXML");
		// Class.forName("com.microsoft.sqlserver.jdbc.IntColumnFilter$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerStatementColumnEncryptionSetting");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SSType");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResultSet$1CursorInitializer");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$DecimalNumericStrategy");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.CekTableEntry");
		// Class.forName("com.microsoft.sqlserver.jdbc.JDBCType$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDS");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection$CityHash128Key");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection$SqlFedAuthInfo");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerStatement$StmtExecOutParamHandler");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResource_es");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerMetaData");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDataSourceObjectFactory");
		// Class.forName("com.microsoft.sqlserver.jdbc.TimeoutTimer$1");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.LinkedDeque$2");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.Weighers$SingletonWeigher");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.LinkedDeque");
		// Class.forName("com.microsoft.sqlserver.jdbc.FailoverMapSingleton");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerBulkCommon$ColumnMetadata");
		// Class.forName("com.microsoft.sqlserver.jdbc.ServerDTVImpl$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerResultSet");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDataSource$SerializationProxy");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerXAConnection");
		// Class.forName("com.microsoft.sqlserver.jdbc.Column$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResource_ru");
		// Class.forName("com.microsoft.sqlserver.jdbc.StreamTabName");
		// Class.forName("com.microsoft.sqlserver.jdbc.SSLProtocol");
		// Class.forName("com.microsoft.sqlserver.jdbc.StreamLoginAck");
		// Class.forName("com.microsoft.sqlserver.jdbc.Segment");
		// Class.forName("com.microsoft.sqlserver.jdbc.StreamDone");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$12");
		// Class.forName("com.microsoft.sqlserver.jdbc.XMLTDSHeader");
		// Class.forName("com.microsoft.sqlserver.jdbc.IntColumnIdentityFilter");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerParameterMetaData$MetaInfo");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo");
		// Class.forName("com.microsoft.sqlserver.jdbc.KeyStoreProviderCommon");
		// Class
		// 	.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$DrainStatus$2");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.XidImpl");
		// Class.forName("com.microsoft.sqlserver.jdbc.DescribeParameterEncryptionResultSet2");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$6");
		// Class.forName("com.microsoft.sqlserver.jdbc.dataclassification.ColumnSensitivity");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSChannel$ProxyInputStream");
		// Class
		// 	.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$BoundedEntryWeigher");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerClobAsciiOutputStream");
		// Class.forName("com.microsoft.sqlserver.jdbc.JavaType");
		// Class
		// 	.forName("com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement$1PreparedHandleClose");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.ParameterUtils");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerBlobOutputStream");
		// Class
		// 	.forName("com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement$PrepStmtBatchExecCmd");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerSpatialDatatype$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerEntityResolver");
		// Class.forName("com.microsoft.sqlserver.jdbc.SocketFinder$Result");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerCallableStatement$1OutParamHandler");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection$LogonCommand");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSReader$1");
		// Class
		// 	.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$DrainStatus$1");
		
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.EvictionListener");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLJdbcVersion");
		// Class.forName("com.microsoft.sqlserver.jdbc.DTV");
		// Class.forName("com.microsoft.sqlserver.jdbc.Geometry");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.JDBCType$NormalizationAE");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerSavepoint");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSTokenHandler");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerBulkCopy$1");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$Node");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.EncryptionKeyInfo");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResource_ko");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$11");
		// Class.forName("com.microsoft.sqlserver.jdbc.UninterruptableTDSCommand");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerClobBase");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerBulkCommon");
		// Class.forName("com.microsoft.sqlserver.jdbc.JDBCType$Category");
		// Class.forName("microsoft.sql.DateTimeOffset");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSChannel$HostNameOverrideX509TrustManager");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$13");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriverIntProperty");
		// Class.forName("com.microsoft.sqlserver.jdbc.Parameter$1");
		// Class
		// 	.forName("com.microsoft.sqlserver.jdbc.SQLServerResultSet$FetchBuffer$FetchBufferTokenHandler");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResultSet$FetchBuffer");
		// Class.forName("com.microsoft.sqlserver.jdbc.JDBCSyntaxTranslator$State");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerEncryptionAlgorithm");
		// Class
		// 	.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$DrainStatus$3");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerEncryptionType");
		// Class
		// 	.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection$FederatedAuthenticationFeatureExtensionData");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource");
		// Class.forName("com.microsoft.sqlserver.jdbc.CertificateDetails");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriverStringProperty");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$3");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerXADataSource$SerializationProxy");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerEncryptionAlgorithmFactory");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSParser");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLState");
		// Class.forName("com.microsoft.sqlserver.jdbc.ActivityCorrelator");
		// Class.forName("com.microsoft.sqlserver.jdbc.GregorianChange");
		// Class.forName("com.microsoft.sqlserver.jdbc.ParsedSQLCacheItem");
		// Class.forName("com.microsoft.sqlserver.jdbc.Point");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResultSet$1CloseServerCursorCommand");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SqlAuthentication");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$8");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResultSet$CursorFetchCommand");
		// Class.forName("com.microsoft.sqlserver.jdbc.TVP$MPIState");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$Values");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.DTVExecuteOp");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.Weigher");
		// Class.forName("com.microsoft.sqlserver.jdbc.JavaType$SetterConversionAE");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDataColumn");
		// Class.forName("com.microsoft.sqlserver.jdbc.StreamRetValue");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResultSet$1ServerCursorInitializer");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.sqlVariantProbBytes");
		// Class.forName("com.microsoft.sqlserver.jdbc.JaasConfiguration");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSReaderMark");
		// Class.forName("com.microsoft.sqlserver.jdbc.KeyStoreAuthentication");
		// Class
		// 	.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$RemovalTask");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerResultSetMetaData");
		// Class.forName("com.microsoft.sqlserver.jdbc.BaseInputStream");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResultSetMetaData$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.dataclassification.SensitivityClassification");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement$1");
		// Class
		// 	.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$DiscardingQueue");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.DTV$SendByRPCOp");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResource_zh_TW");
		// Class.forName("com.microsoft.sqlserver.jdbc.DataTypes$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerBulkCopy$1InsertBulk");
		// Class
		// 	.forName("com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement$1PrepStmtExecOutParamHandler");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResultSet");
		// Class.forName("com.microsoft.sqlserver.jdbc.ApplicationIntent");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerSpatialDatatype");
		// Class.forName("com.microsoft.sqlserver.jdbc.FedAuthDllInfo");
		// Class.forName("com.microsoft.sqlserver.jdbc.SqlVariant");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$9");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResultSet$1DeleteRowRPC");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSType");
		// Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerDataRecord");
		// Class.forName("com.microsoft.sqlserver.jdbc.CacheClear");
		// Class.forName("META-INF.maven.com.microsoft.sqlserver.mssql-jdbc.pom.properties");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerStatement$1NextResult");
		// Class.forName("com.microsoft.sqlserver.jdbc.dns.DNSRecordSRV");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSChannel$SSLHandshakeInputStream");
		// Class.forName("com.microsoft.sqlserver.jdbc.Parameter");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerXAResource");
		// Class.forName("com.microsoft.sqlserver.jdbc.StreamRetStatus");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSCommand");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerNClob");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSPacket");
		// Class.forName("com.microsoft.sqlserver.jdbc.XAReturnValue");
		// Class.forName("com.microsoft.sqlserver.jdbc.DLLException");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$Strategy");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolProxy");
		// Class.forName("com.microsoft.sqlserver.jdbc.Util");
		// Class.forName("com.microsoft.sqlserver.jdbc.SSType$Category");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDatabaseMetaData");
		// Class.forName("com.microsoft.sqlserver.jdbc.UDTTDSHeader");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection43");
		// Class.forName("com.microsoft.sqlserver.jdbc.ActivityId");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$14");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerAeadAes256CbcHmac256Algorithm");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions");
		// Class.forName("com.microsoft.sqlserver.jdbc.SocketConnector");
		// Class.forName("com.microsoft.sqlserver.jdbc.Figure");
		// Class.forName("com.microsoft.sqlserver.jdbc.JDBCType$SetterConversion");
		// Class.forName("com.microsoft.sqlserver.jdbc.TVPType");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.LinkedDeque$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDataTable");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDatabaseMetaData$CallableHandles");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerCallableStatement");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriverPropertyInfo");
		// Class.forName("com.microsoft.sqlserver.jdbc.StreamSetterArgs");
		// Class.forName("com.microsoft.sqlserver.jdbc.Geography");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResultSet$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.DTV$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResource_zh_CN");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerBlob");
		// Class
		// 	.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$KeyIterator");
		
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.Weighers");
		// Class
		// 	.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$DiscardingListener");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerClobWriter");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerSymmetricKeyCache$1");
		// Class
		// 	.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection$PreparedStatementCacheEvictionListener");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.AppDTVImpl");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerClob");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.AuthenticationJNI");
		// Class.forName("com.microsoft.sqlserver.jdbc.JDBCSyntaxTranslator$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.AppDTVImpl$SetValueOp");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDatabaseMetaData$HandleAssociation");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriverObjectProperty");
		// Class.forName("com.microsoft.sqlserver.jdbc.dns.DNSKerberosLocator");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolProxy$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResultSet$1InsertRowRPC");
		// Class.forName("com.microsoft.sqlserver.jdbc.StreamSSPI");
		// Class.forName("com.microsoft.sqlserver.jdbc.KeyVaultCredential");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$EntrySet");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResource_sv");
		// Class.forName("META-INF.services.java.sql.Driver");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnectionSecurityManager");
		// Class.forName("com.microsoft.sqlserver.jdbc.CryptoMetadata");
		// Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerConnection43");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerBulkBatchInsertRecord");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement");
		// Class.forName("com.microsoft.sqlserver.jdbc.PLPXMLInputStream");
		// Class.forName("com.microsoft.sqlserver.jdbc.KerbAuthentication$RealmValidator");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSWriter$TdsOrderUnique");
		// Class
		// 	.forName("com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionAzureKeyVaultProvider");
		
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.Weighers$SetWeigher");
		// Class.forName("com.microsoft.sqlserver.jdbc.JavaType$2");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerSymmetricKeyCache");
		// Class.forName("com.microsoft.sqlserver.jdbc.ColumnFilter");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerSecurityUtility");
		// Class.forName("com.microsoft.sqlserver.jdbc.ReaderInputStream");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLCollation$SortOrder");
		// Class
		// 	.forName("com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource$SerializationProxy");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.UTC");
		// Class.forName("com.microsoft.sqlserver.jdbc.ZeroFixupFilter");
		// Class.forName("com.microsoft.sqlserver.jdbc.Parameter$GetTypeDefinitionOp");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResource_de");
		// Class.forName("com.microsoft.sqlserver.jdbc.JDBCSyntaxTranslator");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLCollation$WindowsLocale");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerParameterMetaData$QueryMeta");
		// Class.forName("mssql.googlecode.cityhash.CityHash");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$2");
		// Class.forName("com.microsoft.sqlserver.jdbc.AsciiFilteredUnicodeInputStream");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerSymmetricKey");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerCallableStatement$1ExecDoneHandler");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerStatement");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerSortOrder");
		// Class
		// 	.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$ValueIterator");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.ThreePartName");
		// Class
		// 	.forName("com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionCertificateStoreProvider");
		
		// Class
		// 	.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$SerializationProxy");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.TVP");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.Weighers$EntryWeigherView");
		// Class.forName("com.microsoft.sqlserver.jdbc.DataTypeFilter");
		// Class.forName("com.microsoft.sqlserver.jdbc.DDC$1");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.Weighers$CollectionWeigher");
		// Class.forName("META-INF.maven.com.microsoft.sqlserver.mssql-jdbc.pom.xml");
		// Class.forName("com.microsoft.sqlserver.jdbc.PLPInputStream");
		// Class.forName("microsoft.sql.DateTimeOffset$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.ScrollWindow");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection$PreparedStatementHandle");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSChannel$ProxySocket");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerResource_ja");
		// Class
		// 	.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$WeightedValue");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.StringUtils");
		// Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerPreparedStatement");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerXADataSource");
		// Class.forName("com.microsoft.sqlserver.jdbc.TypeInfo$Builder$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection$1LogonProcessor");
		// Class.forName("com.microsoft.sqlserver.jdbc.IntColumnFilter");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerBulkCopy");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap$Builder");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.JDBCType$UpdaterConversion");
		// Class.forName("microsoft.sql.Types");
		// Class.forName("com.microsoft.sqlserver.jdbc.ByteArrayOutputStreamToInputStream");
		// Class.forName("com.microsoft.sqlserver.jdbc.StreamType");
		// Class.forName("com.microsoft.sqlserver.jdbc.TVP$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.TDSChannel$PermissiveX509TrustManager");
		// Class
		// 	.forName("com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionJavaKeyStoreProvider");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SSType$GetterConversion");
		// Class.forName("com.microsoft.sqlserver.jdbc.dataclassification.InformationType");
		// Class.forName("com.microsoft.sqlserver.jdbc.KerbAuthentication$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.SimpleInputStream");
		// Class.forName("com.microsoft.sqlserver.jdbc.IntColumnIdentityFilter$1");
		// Class.forName("com.microsoft.sqlserver.jdbc.dataclassification.SensitivityProperty");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerADAL4JUtils");
		// Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionKeyStoreProvider");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerDataSource");
		// Class.forName("com.microsoft.sqlserver.jdbc.KerbAuthentication");
		// Class.forName("mssql.googlecode.concurrentlinkedhashmap.Weighers$SingletonEntryWeigher");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.StreamColumns");
		// Class
		// 	.forName("mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMapEntryIterator");
		
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerStatement$StmtExecCmd");
		// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerAeadAes256CbcHmac256Factory");
		// Class
		// 	.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection$ActiveDirectoryAuthentication");
		
		// } catch (ClassNotFoundException e) {
		// 	throw new RuntimeException(
		// 		"Failed to load MSSql JDBC driver - please ensure 'com.microsoft.sqlserver.jdbc.SQLServerDataSource' jar is included");
		// }
		
		// Setup the configured
		hconfig.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDataSource");
		
		// // Setup the JDBC URL & username and password
		hconfig.setJdbcUrl("jdbc:sqlserver://" + host + ":" + port);
		hconfig.setUsername(user);
		hconfig.setPassword(pass);
		
		// Setup via datasource config
		// hconfig.addDataSourceProperty("serverName", host);
		// hconfig.addDataSourceProperty("port", port);
		// hconfig.addDataSourceProperty("url", "jdbc:sqlserver://" + host + ":" + port
		// 	+ ";databaseName=" + name + ";user=" + user + ";password=" + pass);
		// hconfig.addDataSourceProperty("user", user);
		// hconfig.addDataSourceProperty("password", pass);
		
		// Database name support
		hconfig.addDataSourceProperty("databaseName", name);
		
		// Disable error prone CLOBs
		hconfig.addDataSourceProperty("uselobs", false);
		
		// Initialize the data source and return
		return new HikariDataSource(hconfig);
	}
}
