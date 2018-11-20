package picoded.dstack.connector.jsql;

/**
 * HikariCP utility class 
 **/
class HikaricpUtil {

	// static memoizer for the default max pool size
	static int _defaultMaxPoolSize = -1;

	/**
	 * Default max pool size is always atleast 2
	 * 
	 * Default max pool size will be tagged to the total avaliableProcessors count.
	 * Which would 2 * number of cores, for a hyper threading environment.
	 * 
	 * Coinceidentally, this is the optimal guideline for max coonection pool sizing
	 * See : https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing
	 * 
	 * If hyperthreading is disabled, this estimate is ineffective. But is considered out of scope.
	 * 
	 * @return default max pool size to use
	 */
	static int defaultMaxPoolSize() {
		// Current pool size has not been calculated - calculate it
		if( _defaultMaxPoolSize <= 0 ) {
			_defaultMaxPoolSize = Math.max(2, Runtime.getRuntime().availableProcessors());
		}
		return _defaultMaxPoolSize;
	}
	
}