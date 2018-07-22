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
	
}