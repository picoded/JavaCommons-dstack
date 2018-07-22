package picoded.dmodule.account;

import java.util.*;
import java.util.function.BiFunction;

import picoded.dstack.*;
import picoded.dmodule.*;
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
					accountLoginNameMap.remove(name, oid);
				}
			}
			
			// Remove login authentication details
			accountAuthMap.remove(oid);

			// Remove account meta information
			accountDataObjectMap.remove(oid);
	
			// Remove thorttling information
			loginThrottlingAttemptMap.remove(oid);
			loginThrottlingExpiryMap.remove(oid);

			// System.out.println("Account Object: " + oid + " has been successfully removed.");
			// @TODO : proper info logger
		}
	
		return null;
	}
	
	// ///////////////////////////////////////////////////////////////////////////
	// //
	// // Additional functionality add on
	// //
	// ///////////////////////////////////////////////////////////////////////////
	
	// /** Returns the accountDataObjectMap
	//  *
	//  * @return accountDataObjectMap
	//  **/
	// public DataObjectMap accountDataObjectMap() {
	// 	return accountDataObjectMap;
	// }
	
	// /** Returns the accountVerificationMap
	//  *
	//  * @return list of accountVerification data
	//  **/
	// public KeyValueMap accountVerificationMap() {
	// 	return accountVerificationMap;
	// }
	
	// /** Returns the accountPasswordTokenMap
	//  *
	//  * @return list of accountPasswordToken data
	//  **/
	// public KeyValueMap accountPasswordTokenMap() {
	// 	return accountPasswordTokenMap;
	// }
	
}