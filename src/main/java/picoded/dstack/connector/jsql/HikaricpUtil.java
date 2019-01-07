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
		ret.setIdleTimeout(config.getLong("idleTimeout", ret.getIdleTimeout()));
		
		// maxLifetime
		ret.setMaxLifetime(config.getLong("maxLifetime", ret.getMaxLifetime()));
		
		// connectionTestQuery
		// **not supported:** we enforce JDBC4 and above drivers (for now)
		
		// maximumPoolSize
		ret.setMaximumPoolSize(config.getInt("maximumPoolSize", defaultMaxPoolSize()));
		
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
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDataSource");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(
				"Failed to load MSSql JDBC driver - please ensure 'com.microsoft.sqlserver.jdbc.SQLServerDataSource' jar is included");
		}
		
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
