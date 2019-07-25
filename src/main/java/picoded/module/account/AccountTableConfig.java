package picoded.module.account;

import java.util.*;
import java.util.function.BiFunction;

import picoded.dstack.*;
import picoded.module.*;
import picoded.core.struct.template.UnsupportedDefaultMap;
import picoded.core.struct.query.utils.CollectionQueryForIDInterface;

/**
 * Some of the core underlying structures and variables config of account table,
 * this is mainly used to help organise out the large segments of data structure
 * setup, and config variables house keeping
 **/
abstract class AccountTableConfig extends ModuleStructure implements
	UnsupportedDefaultMap<String, AccountObject>,
	CollectionQueryForIDInterface<String, AccountObject> {
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Constructor setup : Setup the actual tables, with the various names
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Setup with the given stack and name prefix for data structures
	 **/
	public AccountTableConfig(CommonStack inStack, String inName) {
		super(inStack, inName);
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// DataObject sync settings
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * DataObject parameter name to sync login names into
	 */
	public String syncLoginNameList = "loginNameList";
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Login session settings
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * New session lifespan without token
	 **/
	public int initSessionSetupLifespan = 30;
	
	/**
	 * Race condition buffer for tokens
	 **/
	public int sessionRaceConditionBuffer = 10;
	
	/**
	 * defined login lifetime, default as 3600 seconds (aka 1 hr)
	 **/
	public int loginLifetime = 3600; // 1 hr = 60 (mins) * 60 (seconds) = 3600 seconds
	
	/**
	 * lifetime for http login token required for renewal, 1800 seconds (or half an hour)
	 **/
	public int loginRenewal = loginLifetime / 2; //
	
	/**
	 * Remember me lifetime, default as 2592000 seconds (aka 30 days)
	 **/
	public int rememberMeLifetime = 2592000; // 1 mth ~= 30 (days) * 24 (hrs) * 3600 (seconds in an hr)
	
	/**
	 * Remember me lifetime, default the same as loginRenewal
	 **/
	public int rememberMeRenewal = loginRenewal;
	
	/**
	 * Sets the cookie to be limited to http only
	 **/
	public boolean isHttpOnly = false;
	
	/**
	 * Sets the cookie to be via https only
	 **/
	public boolean isSecureOnly = false;
	
	/**
	 * Sets the cookie namespace prefix
	 **/
	public String cookiePrefix = "account_";
	
	/**
	 * Sets teh cookie domain, defaults is null
	 **/
	public String cookieDomain = null;
	
	/**
	 * The nonce size
	 **/
	public int nonceSize = 22;
	
	/**
	 * Cookie path settings to overwrite, use NULL to use contextPath (as detected)
	 **/
	public String cookiePath = null;
	
	/**
	 * Maximum timeout in seconds, using the default calculateDelay function
	 * 3600 seconds = 1 hour
	 */
	public int calculateDelay_maxLockTimeout = 3600;
	
	// /**
	//  * Load and configures the session config from the given map
	//  */
	// public void loadAuthConfig(Map<String,Object> inConfig) {
	// 	//@TODO : implement config loading
	// }
	
	/**
	 * Utility function to get the configured cookie lifetime, with the relevent settings
	 *
	 * @param  remember configuration boolean
	 *
	 * @return configured lifetime (not expire time)
	 **/
	protected int getLifeTime(boolean rememberMe) {
		if (rememberMe) {
			return rememberMeLifetime;
		} else {
			return loginLifetime;
		}
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Login throttling configuration
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Login throttling lambda function, which can be overwritten for
	 * custom login throttling requirements
	 *
	 * @param   Account object in which login failed
	 * @param   Login attempts failed
	 *
	 * @return  Number of seconds to lock the account,
	 *          0 means an account is not locked
	 *          -1 means an account is locked permenantly
	 **/
	public BiFunction<AccountObject, Long, Long> calculateDelay = (inAO, attempts) -> {
		// Tries - Seconds locked
		// 1,2,3 - 0
		// 4     - 1
		// 5     - 3
		// 6     - 7
		// 7     - 15
		//
		// For a maximum of 1 hour
		return (long) Math.min((Math.pow(2, Math.max(0, attempts - 3)) - 1),
			calculateDelay_maxLockTimeout);
	};
	
	///////////////////////////////////////////////////////////////////////////
	//
	// UnsupportedDefaultMap compliance
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Removes all data, without tearing down setup
	 * (Reimplemented to work around interface conflict)
	 *
	 * This is equivalent of "TRUNCATE TABLE {TABLENAME}"
	 **/
	public void clear() {
		systemSetupInterfaceCollection().forEach(item -> item.clear());
	}
	
}
