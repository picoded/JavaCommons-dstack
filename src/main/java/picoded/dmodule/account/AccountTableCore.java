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
				return new AccountObject((AccountTable)this, _oid);
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
}