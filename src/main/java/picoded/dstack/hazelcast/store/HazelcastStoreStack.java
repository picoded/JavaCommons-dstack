package picoded.dstack.hazelcast.store;

// Java imports
import java.util.HashMap;

// JavaCommons imports
import picoded.core.struct.GenericConvertMap;
import picoded.dstack.core.*;
import picoded.dstack.*;
import picoded.dstack.connector.hazelcast.*;
import picoded.dstack.hazelcast.core.*;

// Hazelcast implementation
import com.hazelcast.core.*;
import com.hazelcast.config.*;

/**
 * In memory persistent storage for hazelcast, used this for data structures when caching in memory eviction is not desirec
 * (eg. session keys, lock keys)
 */
public class HazelcastStoreStack extends HazelcastStack {
	
	//--------------------------------------------------------------------------
	//
	// Constructor
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Constructor with configuration map
	 */
	public HazelcastStoreStack(GenericConvertMap<String, Object> inConfig) {
		super(inConfig);
	}
	
	/**
	 * Overwriting of `HazelcastStack.setupHazelcastMapConfig` to include no eviction policy
	 * 
	 * @param mConfig                hazelcast map config to setup
	 * @param dataStructureConfig    data structure config to pass over
	 */
	protected void setupHazelcastMapConfig(MapConfig mConfig,
		GenericConvertMap<String, Object> dataStructureConfig) {
		// Inherit the original settings
		super.setupHazelcastMapConfig(mConfig, dataStructureConfig);
		
		//------------------------------------------------------------------------------------
		// NOTE: Map eviction policy is NONE by default, so nothing needs to be done
		// see: https://docs.hazelcast.org/docs/3.3-RC3/manual/html/map-eviction.html
		//------------------------------------------------------------------------------------
	}
}