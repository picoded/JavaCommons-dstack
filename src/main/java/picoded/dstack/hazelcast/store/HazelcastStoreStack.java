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
import com.hazelcast.map.merge.LatestUpdateMapMergePolicy;

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
		//
		// Lets tune storage for safe redundent stores
		//
		
		// Lets configure the merge policy (latest update is default)
		MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
			.setPolicy("com.hazelcast.map.merge.LatestUpdateMapMergePolicy");
		mConfig.setMergePolicyConfig(mergePolicyConfig);
		
		// Backup count
		int backupCount = dataStructureConfig.getInt("backupCount", config.getInt("backupCount", 2));
		int asyncBackupCount = dataStructureConfig.getInt("asyncBackupCount",
			config.getInt("asyncBackupCount", 0));
		
		// Enable or disable readBackupData, default is true IF asyncBackupCount == 0
		boolean readBackupData = dataStructureConfig.getBoolean("readBackupData",
			config.getBoolean("readBackupData", asyncBackupCount == 0));
		
		// Setup the respective configurations
		mConfig.setBackupCount(backupCount);
		mConfig.setAsyncBackupCount(asyncBackupCount);
		mConfig.setReadBackupData(readBackupData);
		
		//------------------------------------------------------------------------------------
		// NOTE: Map eviction policy is NONE by default, so nothing needs to be done
		// see: https://docs.hazelcast.org/docs/3.3-RC3/manual/html/map-eviction.html
		//------------------------------------------------------------------------------------
	}
}