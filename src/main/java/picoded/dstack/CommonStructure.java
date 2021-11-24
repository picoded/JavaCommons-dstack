package picoded.dstack;

// Java imports
import java.util.Map;

// Library imports
import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.GenericConvertHashMap;
import picoded.core.common.SystemSetupInterface;

// Third party imports
import org.apache.commons.lang3.RandomUtils;

/**
 * Minimal interface for all of picoded.dstack implmentation structures.
 * That handles consistent setup / teardown process.
 *
 * This is used mainly internally via DStack, or JStruct, etc.
 **/
public interface CommonStructure extends SystemSetupInterface {
	
	//--------------------------------------------------------------------------
	//
	// Backend system setup / teardown / maintenance
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Sets up the backend storage. If needed.
	 * The SQL equivalent would be "CREATE TABLE {TABLENAME} IF NOT EXISTS"
	 **/
	void systemSetup();
	
	/**
	 * Destroy, Teardown and delete the backend storage. If needed
	 * The SQL equivalent would be "DROP TABLE {TABLENAME}"
	 **/
	void systemDestroy();
	
	/**
	 * Perform maintenance, this is meant for large maintenance jobs.
	 * Such as weekly or monthly compaction. It may or may not be a long
	 * running task, where its use case is backend specific
	 **/
	void maintenance();
	
	/**
	 * Perform increment maintenance, meant for minor changes between requests.
	 *
	 * By default this randomly triggers a maintenance call with 0.2% probability.
	 * The main reason for doing so, is that for many implmentations there may not be
	 * a concept of incremental maintenance, and in many cases its implementor may forget
	 * to actually call a maintenance call. For years.
	 *
	 * Unless the maintenance call is too expensive, (eg more then 2 seconds), having
	 * it randomly trigger and slow down one transaction randomly. Helps ensure everyone,
	 * systems is more performant in overall.
	 *
	 * It is a very controversal decision, however as awsome as your programming or
	 * devops team is. Your client and their actual infrastructure may be "not as awesome"
	 **/
	default void incrementalMaintenance() {
		// 0.2 percent chance of trigering maintenance
		// This is to lower to overall performance cost incrementalMaintenance per request
		int num = RandomUtils.nextInt(0, 1000);
		if (num <= 1) {
			maintenance();
		}
	}
	
	/**
	 * Removes all data, without tearing down setup
	 *
	 * This is equivalent of "TRUNCATE TABLE {TABLENAME}"
	 **/
	void clear();
	
	/**
	 * persistent config mapping implmentation, conceptually in some cases you should be able to
	 * "overwrite this", maybe even load another dstack object for lolz (dun do in production).
	 *
	 * In other cases it may just do nothing (like this one)
	 *
	 * As such it is important for all implmentation depending on this function,
	 * to fallback to a sane default if the value is "not set"
	 *
	 * @return  The configuration map, to update / change settings.
	 **/
	default GenericConvertMap<String, Object> configMap() {
		return new GenericConvertHashMap<String, Object>();
	}
	
}
