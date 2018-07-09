package picoded.dstack.stack;

// Java imports
import java.util.Map;

// Library imports
import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.GenericConvertHashMap;
import picoded.core.common.SystemSetupInterface;
import picoded.dstack.CommonStructure;

// Third party imports
import org.apache.commons.lang3.RandomUtils;

/**
 * Extend CommonStructure implementation for common DStack 
 * setup and maintainaince commands
 **/
public interface Stack_CommonStructure extends CommonStructure {
	
	//--------------------------------------------------------------------------
	//
	// Interface to ovewrite for `Stack_CommonStructure` implmentation
	//
	//--------------------------------------------------------------------------
	
	/**
	 * @return  array of the internal common structure stack used by the Stack_ implementation
	 */
	CommonStructure[] commonStructureStack();
	
	//--------------------------------------------------------------------------
	//
	// Backend system setup / teardown / maintenance
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Sets up the backend storage. If needed.
	 * The SQL equivalent would be "CREATE TABLE {TABLENAME} IF NOT EXISTS"
	 **/
	default void systemSetup() {
		for (CommonStructure layer : commonStructureStack()) {
			layer.systemSetup();
		}
	}
	
	/**
	 * Destroy, Teardown and delete the backend storage. If needed
	 * The SQL equivalent would be "DROP TABLE {TABLENAME}"
	 **/
	default void systemDestroy() {
		for (CommonStructure layer : commonStructureStack()) {
			layer.systemDestroy();
		}
	}
	
	/**
	 * Perform maintenance, this is meant for large maintenance jobs.
	 * Such as weekly or monthly compaction. It may or may not be a long
	 * running task, where its use case is backend specific
	 **/
	default void maintenance() {
		for (CommonStructure layer : commonStructureStack()) {
			layer.maintenance();
		}
	}
	
	/**
	 * Removes all data, without tearing down setup
	 *
	 * This is equivalent of "TRUNCATE TABLE {TABLENAME}"
	 **/
	default void clear() {
		for (CommonStructure layer : commonStructureStack()) {
			layer.clear();
		}
	}
	
}
