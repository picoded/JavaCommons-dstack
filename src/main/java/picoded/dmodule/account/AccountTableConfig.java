package picoded.dmodule.account;

import java.util.*;
import java.util.function.BiFunction;

import picoded.dstack.*;
import picoded.dmodule.*;
import picoded.core.conv.*;
import picoded.core.struct.*;
import picoded.core.struct.template.UnsupportedDefaultMap;

/**
 * Some of the core underlying structures and variables config of account table,
 * this is mainly used to help organise out the large segments of data structure
 * setup, and config variables house keeping
 **/
abstract class AccountTableConfig extends ModuleStructure implements
	UnsupportedDefaultMap<String, AccountObject> {
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Underlying data structures
	//
	///////////////////////////////////////////////////////////////////////////
	
	//
	// Login authentication
	//
	//-----------------------------------------
	
	/**
	 * Provides a key value pair mapping of the account login ID to AccountID (GUID)
	 *
	 * KeyValueMap<uniqueLoginName,AccountID>
	 *
	 * login ID are unique, and are usually usernames or emails
	 * AccountID's are not unique, as a single AccountID can have multiple "names"
	 **/
	protected KeyValueMap accountLoginNameMap = null; //to delete from
	
	/**
	 * Stores the account authentication hash, used for password based authentication
	 *
	 * KeyValueMap<AccountID,passwordHash>
	 **/
	protected KeyValueMap accountAuthMap = null; //to delete from
	
	//
	// Login session
	//
	//-----------------------------------------
	
	/**
	 * Stores the account session key, to accountID link
	 *
	 * KeyValueMap<sessionID, accountID>
	 **/
	protected KeyValueMap sessionLinkMap = null;
	
	/**
	 * Stores the account token key, to session key
	 *
	 * KeyValueMap<tokenID, sessionID>
	 **/
	protected KeyValueMap sessionTokenMap = null;
	
	/**
	 * Stores the next token ID to reissue
	 * This limits race conditions where multiple tokens are issued
	 *
	 * KeyValueMap<tokenID, next-tokenID>
	 **/
	protected KeyValueMap sessionNextTokenMap = null;
	
	// /**
	//  * Stores the account meta information
	//  *
	//  * KeyValueMap<sessionID, info-about-access>
	//  **/
	// protected KeyValueMap sessionInfoMap = null;
	
	//
	// Account meta information
	//
	//-----------------------------------------
	
	/**
	 * Account meta information
	 * Used to pretty much store all individual information
	 * directly associated with the account
	 *
	 * Note: Consider this the "PRIMARY TABLE"
	 *
	 * DataObjectMap<AccountOID, DataObject>
	 **/
	protected DataObjectMap accountDataObjectMap = null;
	
	//
	// Login throttling information
	//
	//-----------------------------------------
	
	/**
	 * Handles the Login Throttling Attempt Key (AccountID) Value (Attempt) field mapping
	 *
	 * KeyLongMap<UserOID, attempts>
	 **/
	protected KeyLongMap loginThrottlingAttemptMap = null;
	
	/**
	 * Handles the Login Throttling Attempt Key (AccountID) Value (Timeout) field mapping
	 *
	 * KeyLongMap<UserOID, expireTimestamp>
	 **/
	protected KeyLongMap loginThrottlingExpiryMap = null;
	
	// //
	// // Account Related Token Maps
	// //
	// //-----------------------------------------
	
	// /**
	//  * Stores the verification token against the account ID together with the expiry time
	//  **/
	// protected KeyValueMap accountVerificationMap = null;
	
	// /**
	//  * Stores the verification token for account password resets with expiry time
	//  **/
	// protected KeyValueMap accountPasswordTokenMap = null;
	
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
		internalStructureList = setupInternalStructureList();
	}
	
	/**
	 * Initialize the various internal data structures, 
	 * used by account from the stack.
	 **/
	protected List<CommonStructure> setupInternalStructureList() {
		
		// Login auth information
		accountLoginNameMap = stack.keyValueMap(name + "_ID");
		accountAuthMap = stack.keyValueMap(name + "_IH");
		
		// Login session infromation
		sessionLinkMap = stack.keyValueMap(name + "_LS");
		sessionTokenMap = stack.keyValueMap(name + "_LT");
		sessionNextTokenMap = stack.keyValueMap(name + "_LN");
		// sessionInfoMap = stack.keyValueMap(name + "_LI");
		
		// Account meta information
		accountDataObjectMap = stack.dataObjectMap(name + "_AM");
		
		// Login throttling information
		loginThrottlingAttemptMap = stack.keyLongMap(name + "_TA");
		loginThrottlingExpiryMap = stack.keyLongMap(name + "_TE");
		
		// // Account Verification information
		// accountVerificationMap = stack.keyValueMap(name + "_AV");
		// // Account Password Token information
		// accountPasswordTokenMap = stack.keyValueMap(name + "_PT");
		
		// Side note: For new table, edit here and add into the return List
		// @TODO - Consider adding support for temporary tables typehints
		
		// Return it as a list
		return Arrays.asList( //
			accountLoginNameMap, accountAuthMap, //
			sessionLinkMap, sessionTokenMap, sessionNextTokenMap, //
			accountDataObjectMap, //
			loginThrottlingAttemptMap, loginThrottlingExpiryMap//, //
			// accountVerificationMap, //
			// accountPasswordTokenMap //
			);
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// DataObject sync settings
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * DataObject parameter name to sync login names into
	 */
	public String syncLoginNameList = "LoginNameList";

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
	
	// /**
	//  * Load and configures the session config from the given map
	//  */
	// public void loadAuthConfig(Map<String,Object> inConfig) {
	// 	//@TODO : implement config loading
	// }
	
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
		return (long) (Math.pow(2, Math.max(0, attempts - 3)) - 1);
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