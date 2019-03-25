package picoded.module.account;

import java.util.*;
import java.util.function.BiFunction;

import picoded.dstack.*;
import picoded.module.*;
import picoded.core.conv.*;
import picoded.core.struct.*;
import picoded.core.struct.template.UnsupportedDefaultMap;

// import static picoded.servlet.api.module.account.AccountConstantStrings.*;

/**
 * Core key features used for account table
 * before the servlet request abstraction layer
 **/
public abstract class AccountTableCore extends AccountTableConfig {
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Constructor setup : Setup the actual tables, with the various names
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Setup with the given stack and name prefix for data structures
	 **/
	public AccountTableCore(CommonStack inStack, String inName) {
		super(inStack, inName);
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Basic account interaction (without AccountObject)
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Returns if the name exists
	 *
	 * @param  Login ID to use, normally this is an email, or nice username
	 *
	 * @return TRUE if login ID exists
	 **/
	public boolean hasLoginName(String inLoginName) {
		return accountLoginNameMap.containsKey(inLoginName);
	}
	
	/**
	 * Returns if the account object id exists
	 *
	 * @param  Account OID to use
	 *
	 * @return  TRUE of account ID exists
	 **/
	public boolean containsKey(Object oid) {
		return accountDataObjectMap.containsKey(oid);
	}
	
	/**
	 * Gets the account UUID, using the configured name
	 *
	 * @param  accountName (nice-name/email)
	 *
	 * @return  Account ID associated, if any
	 **/
	public String loginNameToAccountID(String accountName) {
		return accountLoginNameMap.getValue(accountName);
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Map compliance (without account object)
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Returns all the account _oid in the system
	 *
	 * @return  Set of account oid's
	 **/
	public Set<String> keySet() {
		return accountDataObjectMap.keySet();
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Account object getters
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Gets and return the accounts object using the account ID
	 *
	 * @param  Account ID to use
	 *
	 * @return  AccountObject representing the account ID if found
	 **/
	public AccountObject get(Object oid) {
		// Possibly a valid OID?
		if (oid != null) {
			String _oid = oid.toString();
			if (containsKey(_oid)) {
				return new AccountObject(this, _oid);
			}
		}
		// Account object invalid here
		return null;
	}
	
	/**
	 * Gets the account using the nice name
	 *
	 * @param  The login ID (nice-name/email)
	 *
	 * @return  AccountObject representing the account ID if found
	 **/
	public AccountObject getFromLoginName(Object name) {
		String _oid = loginNameToAccountID(name.toString());
		if (_oid != null) {
			return get(_oid);
		}
		return null;
	}
	
	/**
	 * Gets the account using the Session ID
	 *
	 * @param  The Session ID
	 *
	 * @return  AccountObject representing the account ID if found
	 **/
	public AccountObject getFromSessionID(String sessionID) {
		String _oid = sessionLinkMap.getValue(sessionID);
		if (_oid != null) {
			return get(_oid);
		}
		return null;
	}
	
	/**
	 * Gets the account using the object ID array,
	 * and returns an account object array
	 *
	 * @param   Account object ID array
	 *
	 * @return  Array of corresponding account objects
	 **/
	public AccountObject[] getFromArray(String[] _oidList) {
		AccountObject[] mList = new AccountObject[_oidList.length];
		for (int a = 0; a < _oidList.length; ++a) {
			mList[a] = get(_oidList[a]);
		}
		return mList;
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Account object "newEntry"
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Generates a new account object.
	 *
	 * Note without setting a name, or any additional values.
	 * This call in some sense is quite, err useless.
	 **/
	public AccountObject newEntry() {
		AccountObject ret = new AccountObject(this, null);
		// ret.saveAll(); //ensures the blank object is now in DB
		return ret;
	}
	
	/**
	 * Generates a new account object with the given nice name
	 *
	 * @param  Unique Login ID to use, normally this is an email, or nice username
	 *
	 * @return AccountObject if succesfully created
	 **/
	public AccountObject newEntry(String name) {
		// Quick fail check
		if (hasLoginName(name)) {
			return null;
		}
		
		// Creating account object, setting the name if valid
		AccountObject ret = newEntry();
		if (ret.setLoginName(name)) {
			return ret;
		} else {
			// Removal step is required on failure,
			// as it helps prevent "orphaned" account objects
			// in new account race conditions
			remove(ret._oid());
		}
		
		// Return null on failure
		return null;
	}
	
	/**
	 * Removes the accountObject using the ID
	 *
	 * @param  Account OID to use, or alternatively its object
	 *
	 * @return NULL
	 **/
	public AccountObject remove(Object inOid) {
		if (inOid != null) {
			
			// Alternatively, instead of string use DataObject
			if (inOid instanceof DataObject) {
				inOid = ((DataObject) inOid)._oid();
			}
			
			// Get oid as a string, and fetch the account object
			String oid = inOid.toString();
			// AccountObject ao = this.get(oid);
			
			// Remove login ID's AKA nice names
			Set<String> loginIdMapNames = accountLoginNameMap.keySet(oid);
			if (loginIdMapNames != null) {
				for (String name : loginIdMapNames) {
					accountLoginNameMap.remove(name);
				}
			}
			
			// Remove login authentication details
			accountAuthMap.remove(oid);
			
			// Remove account meta information
			accountDataObjectMap.remove(oid);
			
			// Remove thorttling information
			loginThrottlingAttemptMap.remove(oid);
			loginThrottlingExpiryMap.remove(oid);
			
			// @TODO: Remove things from Verification and Password Token map?
			
			// System.out.println("Account Object: " + oid + " has been successfully removed.");
			// @TODO : proper info logger
		}
		
		return null;
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// CollectionQueryForIDInterface based support
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Performs a search query, and returns the respective AccountObject keys.
	 *
	 * This is the GUID key varient of query.
	 *
	 * @param   where query statement
	 * @param   where clause values array
	 * @param   query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max, use -1 to ignore
	 *
	 * @return  The String[] array
	 **/
	public String[] query_id(String whereClause, Object[] whereValues, String orderByStr,
		int offset, int limit) {
		return accountDataObjectMap.query_id(whereClause, whereValues, orderByStr, offset, limit);
	}
	
	/**
	 * Performs a search query, and returns the respective AccountObject
	 *
	 * @param   where query statement
	 * @param   where clause values array
	 * @param   query string to sort the order by, use null to ignore
	 * @param   offset of the result to display, use -1 to ignore
	 * @param   number of objects to return max, use -1 to ignore
	 *
	 * @return  The String[] array
	 **/
	public AccountObject[] query(String whereClause, Object[] whereValues, String orderByStr,
		int offset, int limit) {
		// @TOCONSIDER - to have an unchecked implementation of getFromArray, and to use that instead? 
		return getFromArray(accountDataObjectMap.query_id(whereClause, whereValues, orderByStr,
			offset, limit));
	}
	
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
	protected KeyValueMap accountLoginNameMap; //to delete from
	
	/**
	 * Stores the account authentication hash, used for password based authentication
	 *
	 * KeyValueMap<AccountID,passwordHash>
	 **/
	protected KeyValueMap accountAuthMap; //to delete from
	
	//
	// Login session
	//
	//-----------------------------------------
	
	/**
	 * Stores the account session key, to accountID link
	 *
	 * KeyValueMap<sessionID, accountID>
	 **/
	protected KeyValueMap sessionLinkMap;
	
	/**
	 * Stores the account token key, to session key
	 *
	 * KeyValueMap<tokenID, sessionID>
	 **/
	protected KeyValueMap sessionTokenMap;
	
	/**
	 * Stores the next token ID to reissue
	 * This limits race conditions where multiple tokens are issued
	 *
	 * KeyValueMap<tokenID, next-tokenID>
	 **/
	protected KeyValueMap sessionNextTokenMap;
	
	/**
	 * Stores the account meta information
	 *
	 * KeyValueMap<sessionID, info-about-access>
	 **/
	protected KeyValueMap sessionInfoMap;
	
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
	protected DataObjectMap accountDataObjectMap;
	
	//
	// Login throttling information
	//
	//-----------------------------------------
	
	/**
	 * Handles the Login Throttling Attempt Key (AccountID) Value (Attempt) field mapping
	 *
	 * KeyLongMap<UserOID, attempts>
	 **/
	protected KeyLongMap loginThrottlingAttemptMap;
	
	/**
	 * Handles the Login Throttling Attempt Key (AccountID) Value (Timeout) field mapping
	 *
	 * KeyLongMap<UserOID, expireTimestamp>
	 **/
	protected KeyLongMap loginThrottlingExpiryMap;
	
	//
	// Account Related Token Maps
	//
	//-----------------------------------------
	
	/**
	 * Stores the verification token against the account ID together with the expiry time
	 **/
	protected KeyValueMap accountVerificationMap;
	
	/**
	 * Stores the verification token for account password resets with expiry time
	 **/
	protected KeyValueMap accountPasswordResetTokenMap;
	
	/**
	 * Setup data structure from stack using default naming
	 */
	protected void setupDStackMaps() {
		// Login auth information
		accountLoginNameMap = stack.keyValueMap(name + "_ID");
		accountAuthMap = stack.keyValueMap(name + "_IH");
		
		// Login session infromation
		sessionLinkMap = stack.keyValueMap(name + "_SL");
		sessionTokenMap = stack.keyValueMap(name + "_ST");
		sessionNextTokenMap = stack.keyValueMap(name + "_SN");
		sessionInfoMap = stack.keyValueMap(name + "_SI");
		
		// Account meta information
		accountDataObjectMap = stack.dataObjectMap(name + "_AM");
		
		// Login throttling information
		loginThrottlingAttemptMap = stack.keyLongMap(name + "_TA");
		loginThrottlingExpiryMap = stack.keyLongMap(name + "_TE");
		
		// Account Verification information
		accountVerificationMap = stack.keyValueMap(name + "_AV");
		// Account Password Token information
		accountPasswordResetTokenMap = stack.keyValueMap(name + "_AR");
		
		// Side note: For new table, edit here and add into the return List
		// @TODO - Consider adding support for temporary tables typehints
	}
	
	/**
	 * Initialize the various internal data structures,
	 * used by account from the stack.
	 **/
	protected List<CommonStructure> setupInternalStructureList() {
		
		setupDStackMaps();
		
		// Return it as a list
		return Arrays.asList( //
			accountLoginNameMap, accountAuthMap, //
			sessionLinkMap, sessionTokenMap, sessionNextTokenMap, sessionInfoMap, //
			accountDataObjectMap, //
			loginThrottlingAttemptMap, loginThrottlingExpiryMap, //
			accountVerificationMap, accountPasswordResetTokenMap //
			);
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Additional functionality add on
	//
	///////////////////////////////////////////////////////////////////////////
	
	/** Returns the accountDataObjectMap
	 *
	 * @return accountDataObjectMap
	 **/
	public DataObjectMap accountDataObjectMap() {
		return accountDataObjectMap;
	}
	
	/** Returns the accountVerificationMap
	 *
	 * @return list of accountVerification data
	 **/
	public KeyValueMap accountVerificationMap() {
		return accountVerificationMap;
	}
	
	/** Returns the accountPasswordResetTokenMap
	 *
	 * @return list of accountPasswordToken data
	 **/
	public KeyValueMap accountPasswordResetTokenMap() {
		return accountPasswordResetTokenMap;
	}
}
