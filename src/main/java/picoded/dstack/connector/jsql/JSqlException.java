package picoded.dstack.connector.jsql;

/**
 * JSql exception wrapper
 **/
public class JSqlException extends RuntimeException {
	protected static final long serialVersionUID = 1L;
	
	public static final String invalidDatabaseImplementationException = "Invalid JSql"
		+ " implementation. Please use the resepctive database implementations,"
		+ "and avoid initiating the JSql class directly";
	
	public static final String oracleNameSpaceWarning = "Table/Index/View/Column"
		+ " name should not be more then 30 char (due to lack of ORACLE support): ";
	
	public JSqlException(String message) {
		super(message);
	}
	
	public JSqlException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public JSqlException(Throwable cause) {
		super("JSqlException", cause);
	}
}
