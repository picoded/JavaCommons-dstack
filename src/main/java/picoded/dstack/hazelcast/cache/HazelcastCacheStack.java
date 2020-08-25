package picoded.dstack.hazelcast.cache;

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

// import com.hazelcast.map.eviction.*;

/**
 * In memory persistent storage for hazelcast, used this for data structures when caching in memory eviction is not desirec
 * (eg. session keys, lock keys)
 */
public class HazelcastCacheStack extends HazelcastStack {
	
	//--------------------------------------------------------------------------
	//
	// Constructor
	//
	//--------------------------------------------------------------------------
	
	/**
	 * Constructor with configuration map
	 */
	public HazelcastCacheStack(GenericConvertMap<String, Object> inConfig) {
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
			.setPolicy("com.hazelcast.spi.merge.LatestUpdateMergePolicy");
		mConfig.setMergePolicyConfig(mergePolicyConfig);
		
		//---------------------------------------------------------------
		// Backup is tuned for cache based backend
		//---------------------------------------------------------------
		
		//
		// Lets get some of the key settings
		//
		
		// Backup count
		int backupCount = dataStructureConfig.getInt("backupCount", config.getInt("backupCount", 0));
		int asyncBackupCount = dataStructureConfig.getInt("asyncBackupCount",
			config.getInt("asyncBackupCount", 1));
		
		// Enable or disable readBackupData, default is true IF asyncBackupCount == 0
		boolean readBackupData = dataStructureConfig.getBoolean("readBackupData",
			config.getBoolean("readBackupData", asyncBackupCount == 0));
		
		// Setup the respective configurations
		mConfig.setBackupCount(backupCount);
		mConfig.setAsyncBackupCount(asyncBackupCount);
		mConfig.setReadBackupData(readBackupData);
		
		//---------------------------------------------------------------
		// Eviction policy controls 
		//---------------------------------------------------------------
		
		// Eviction config to setup
		EvictionConfig eConfig = new EvictionConfig();
		
		// Get required settings
		int freeHeapPercentage = dataStructureConfig.getInt("freeHeapPercentage",
			config.getInt("freeHeapPercentage", 20));
		
		// Configure max size policy percentage to JVM heap
		eConfig.setEvictionPolicy(EvictionPolicy.LRU);
		eConfig.setMaxSizePolicy(MaxSizePolicy.FREE_HEAP_PERCENTAGE);
		eConfig.setSize(freeHeapPercentage);
		
		// Set the eviction config
		mConfig.setEvictionConfig(eConfig);
		
		// Configure default max idle seconds
		// Note, 3600s -> 1 hour
		int maxIdleSeconds = dataStructureConfig.getInt("maxIdleSeconds",
			config.getInt("maxIdleSeconds", 0));
		mConfig.setMaxIdleSeconds(maxIdleSeconds);
		
	}
}