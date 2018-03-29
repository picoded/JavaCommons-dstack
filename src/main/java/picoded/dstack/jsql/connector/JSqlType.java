package picoded.dstack.jsql.connector;

/**
 * JSql type options enum, see JSql.sqlType for its usage
 **/
public enum JSqlType {
	
	/**
	 * invalid type (reserved for base class)
	 **/
	INVALID,
	/**
	 * Others
	 **/
	OTHERS,
	/**
	 * sqlite jdbc or file access
	 **/
	SQLITE,
	/**
	 * mysql connection mode
	 **/
	MYSQL,
	/**
	 * oracle connection mode
	 **/
	ORACLE,
	/**
	 * MS-Sql connection mode
	 **/
	MSSQL,
	/**
	 * Postgres connection mode
	 **/
	POSTGRESQL,
	/**
	 * DB2 connection mode
	 **/
	DB2;
	/**
	 * Get name and toString alias to name() varient
	 **/
	public String getName() {
		return super.name();
	}
	
	/**
	 * Get name and toString alias to name() varient
	 **/
	public String toString() {
		return super.name();
	}
	
	/**
	 * TypeMap to be extended, and stored in their respective package usage
	 **/
	public static class JSqlTypeSet {
		public static final JSqlType INVALID = JSqlType.INVALID;
		public static final JSqlType OTHERS = JSqlType.OTHERS;
		
		public static final JSqlType SQLITE = JSqlType.SQLITE;
		public static final JSqlType MYSQL = JSqlType.MYSQL;
		public static final JSqlType ORACLE = JSqlType.ORACLE;
		public static final JSqlType MSSQL = JSqlType.MSSQL;
	}
	
	/**
	 * Byte to enum serialization
	 **/
	public static JSqlType toEnum(int val) {
		switch (val) {
		case 0:
			return JSqlType.INVALID;
		case 1:
			return JSqlType.OTHERS;
		case 2:
			return JSqlType.SQLITE;
		case 3:
			return JSqlType.MYSQL;
		case 4:
			return JSqlType.ORACLE;
		case 5:
			return JSqlType.MSSQL;
		case 6:
			return JSqlType.POSTGRESQL;
		case 7:
			return JSqlType.DB2;
		}
		return null;
	}
	
	/**
	 * Enum to byte serialization
	 **/
	public static int toInt(JSqlType val) {
		switch (val) {
		case INVALID:
			return 0;
		case OTHERS:
			return 1;
		case SQLITE:
			return 2;
		case MYSQL:
			return 3;
		case ORACLE:
			return 4;
		case MSSQL:
			return 5;
		case POSTGRESQL:
			return 6;
		case DB2:
			return 7;
		}
		return -1;
	}
}
