package picoded.module.account;

import java.util.*;

import picoded.dstack.*;
import picoded.dstack.core.*;
import picoded.core.conv.*;
import picoded.core.struct.*;
import picoded.core.security.NxtCrypt;

// import static picoded.servlet.api.module.account.AccountConstantStrings.*;

/**
 * Represents a single group / user account.
 **/
public class AccountObject extends Core_DataObject {
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Constructor and setup
	//
	///////////////////////////////////////////////////////////////////////////
	//#region constructor and setup
	
	/**
	 * The original account table
	 **/
	protected AccountTable mainTable = null;
	
	/**
	 * [INTERNAL USE ONLY]
	 *
	 * Cosntructor setup, using an account table,
	 * and the account GUID
	 **/
	protected AccountObject(AccountTable accTable, String inOID) {
		// Inherit all the default data table methods
		super((Core_DataObjectMap) (accTable.accountDataObjectMap), inOID);
		mainTable = accTable;
	}
	
	/**
	 * [INTERNAL USE ONLY]
	 *
	 * Cosntructor setup, using an account table,
	 * and the account GUID
	 **/
	protected AccountObject(AccountTableCore accTable, String inOID) {
		this((AccountTable) accTable, inOID);
	}
	
	//#endregion constructor and setup
	///////////////////////////////////////////////////////////////////////////
	//
	// Getting login ID's
	//
	///////////////////////////////////////////////////////////////////////////
	//#region login id getters
	
	/**
	 * Checks if the current account has the provided LoginName
	 *
	 * @param  LoginName to use
	 *
	 * @return TRUE if login ID belongs to this account
	 **/
	public boolean hasLoginName(String name) {
		return _oid.equals(mainTable.accountLoginNameMap.get(name).getValue());
	}
	
	/**
	 * Gets and return the various login "nice-name" (not UUID) for this account
	 *
	 * @return  Set of LoginName's used by this account
	 **/
	public Set<String> getLoginNameSet() {
		return mainTable.accountLoginNameMap.keySet(_oid);
	}
	
	//#endregion login id getters
	///////////////////////////////////////////////////////////////////////////
	//
	// Syncronsing data from authentication related tables to
	// queryable data table objects.
	//
	// NOTE: This is not actually used for authentication,
	//       but for convienience in places such as admin tables
	//
	///////////////////////////////////////////////////////////////////////////
	//#region auth tables data sync
	
	/**
	 * Syncs the current user name list, with its metaobject
	 * This is mainly for data table listing "convinence"
	 */
	protected void syncLoginNameList() {
		// Only perform mainTable.syncLoginNameList if configured
		if (mainTable.syncLoginNameList == null || mainTable.syncLoginNameList.length() <= 0) {
			return;
		}
		
		// Get the raw name list
		Set<String> rawList = getLoginNameSet();
		
		// Sort it out as an array
		ArrayList<String> nameList = new ArrayList<>(rawList);
		Collections.sort(nameList);
		
		// Update the meta data
		this.put(mainTable.syncLoginNameList, nameList);
		
		// Save the changes
		this.saveDelta();
	}
	
	//#endregion auth tables data sync
	///////////////////////////////////////////////////////////////////////////
	//
	// Setting login ID's
	//
	///////////////////////////////////////////////////////////////////////////
	//#region login id handling
	
	/**
	 * Sets the name for the account, returns true or false if it succeded.
	 *
	 * @param  LoginName to setup for this account
	 *
	 * @return TRUE if login ID is configured to this account
	 **/
	public boolean setLoginName(String name) {
		// Argument checks
		if (name == null || name.trim().length() <= 0) {
			throw new IllegalArgumentException("AccountObject login name cannot be blank");
		}
		
		// Name trim safety
		name = name.trim();
		
		// Quick fail check
		if (mainTable.hasLoginName(name)) {
			return false;
		}
		
		// ensure its own OID is registered
		saveDelta();
		
		// Technically a race condition =X
		// Especially if its a name collision, if its an email collision should be a very rare event.
		//
		// @TODO : Consider using name locks? to prevent such situations?
		mainTable.accountLoginNameMap.put(name, _oid);
		
		// Sync up the namelist in dataobject
		syncLoginNameList();
		
		// Success configuration of loginName
		return true;
	}
	
	/**
	 * Removes the old name from the database
	 *
	 * @param  LoginName to setup for this account
	 **/
	public void removeLoginName(String name) {
		// If login name exists
		if (hasLoginName(name)) {
			// Remove name list from authtentication table
			mainTable.accountLoginNameMap.remove(name);
			// Sync up the namelist in dataobject
			syncLoginNameList();
		}
	}
	
	/**
	 * Sets the name as a unique value, delete all previous alias
	 *
	 * @param  LoginName to setup for this account
	 *
	 * @return TRUE if login ID is configured to this account
	 **/
	public boolean setUniqueLoginName(String name) {
		// The old name list, to check if new name already is set
		Set<String> oldNamesList = getLoginNameSet();
		
		// Check if name exist in list
		if (Arrays.asList(oldNamesList).contains(name)) {
			// Already exists in the list, does nothing
		} else {
			// Name does not exist, attempt to set the name
			if (!setLoginName(name)) {
				// Failed to setup the name, terminate
				return false;
			}
		}
		
		// Iterate the names, delete uneeded ones
		for (String oldName : oldNamesList) {
			// Skip the unique name,
			// prevent it from being deleted
			if (oldName.equals(name)) {
				continue;
			}
			// Remove the login ID
			mainTable.accountLoginNameMap.remove(oldName);
		}
		
		// Sync up the namelist in dataobject
		syncLoginNameList();
		return true;
	}
	
	//#endregion login id handling
	///////////////////////////////////////////////////////////////////////////
	//
	// Password management
	//
	///////////////////////////////////////////////////////////////////////////
	//#region password management
	
	/**
	 * Gets and returns the stored password hash,
	 * Intentionally made private to avoid accidental use externally
	 *
	 * @return  Password salted hash, as per NxtCrypt usage
	 **/
	private String getPasswordHash() {
		return mainTable.accountAuthMap.getValue(_oid);
	}
	
	/**
	 * Indicates if the current account has a configured password, 
	 * it is possible there is no password for passwordless login
	 *
	 * @return  True if password was configured
	 **/
	public boolean hasPassword() {
		String h = getPasswordHash();
		return (h != null && h.length() > 0);
	}
	
	/**
	 * Remove the account password
	 **/
	public void removePassword() {
		mainTable.accountAuthMap.remove(_oid);
	}
	
	/**
	 * Validate if the given password is valid
	 *
	 * @param  raw Password string to validate
	 *
	 * @return  True if password is valid
	 **/
	public boolean validatePassword(String pass) {
		String hash = getPasswordHash();
		if (hash != null) {
			return NxtCrypt.validatePassHash(hash, pass);
		}
		return false;
	}
	
	/**
	 * Set the account password
	 *
	 * @param  raw Password string to setup
	 **/
	public void setPassword(String pass) {
		// ensure its own OID is registered
		saveDelta();
		
		// Setup the password
		if (pass == null) {
			removePassword();
		} else {
			mainTable.accountAuthMap.put(_oid, NxtCrypt.getPassHash(pass));
		}
	}
	
	/**
	 * Set the account password, after checking old password
	 *
	 * @param  raw Password string to setup
	 * @param  old Password to validate
	 *
	 * @return  True if password change was valid
	 **/
	public boolean setPassword(String pass, String oldPass) {
		if (validatePassword(oldPass)) {
			setPassword(pass);
			return true;
		}
		return false;
	}
	
	//#endregion password management
	///////////////////////////////////////////////////////////////////////////
	//
	// Login throttling
	//
	///////////////////////////////////////////////////////////////////////////
	//#region login throttling (for password api)
	
	/**
	 * Returns the unix timestamp (in seconds) in which the account will unlock
	 *
	 * @return if > 0, linux timestamp (seconds) when login is permitted. 0 means the account is not locked
	 **/
	public long getUnlockTimestamp() {
		return mainTable.loginThrottlingExpiryMap.getLong(this._oid(), 0l);
	}
	
	/**
	 * Returns the number of failed login attempts performed
	 *
	 * @return Number of login attempts performed since last succesful login
	 **/
	public long getFailedLoginAttempts() {
		return mainTable.loginThrottlingAttemptMap.getLong(this._oid(), 0l);
	}
	
	/**
	 * Reset the entries for the user (should only be called after successful login)
	 **/
	public void resetLoginThrottle() {
		mainTable.loginThrottlingAttemptMap.weakCompareAndSet(this._oid(), getFailedLoginAttempts(),
			0l);
		mainTable.loginThrottlingExpiryMap.weakCompareAndSet(this._oid(), getUnlockTimestamp(), 0l);
	}
	
	/**
	 * Returns time left in seconds before next permitted login attempt for the user based on User ID
	 *
	 * @return if > 0, linux timestamp (seconds) when login is permitted.
	 **/
	public int getLockTimeLeft() {
		long val = getUnlockTimestamp();
		int allowedTime = (int) val - (int) (System.currentTimeMillis() / 1000);
		return allowedTime > 0 ? allowedTime : 0;
	}
	
	/**
	 * Increment the number of failed login attempts, and the lock timeout respectively
	 **/
	public void incrementFailedLoginAttempts() {
		long attemptValue = mainTable.loginThrottlingAttemptMap.getAndIncrement(this._oid());
		int elapsedValue = (int) (System.currentTimeMillis() / 1000);
		elapsedValue += mainTable.calculateDelay.apply(this, attemptValue);
		mainTable.loginThrottlingExpiryMap.weakCompareAndSet(this._oid(), getUnlockTimestamp(),
			(long) elapsedValue);
	}
	
	//#endregion login throttling (for password api)
	///////////////////////////////////////////////////////////////////////////
	//
	// Session management
	//
	///////////////////////////////////////////////////////////////////////////
	//
	// Session vs Token notes
	// ----------------------
	//
	// Session : represents a sucessful login attempt,
	// which does not change and can only be extended.
	//
	// Token : is issued through a session, and gets replaced with newer
	// tokens over time, extending the session in the process.
	//
	// Tokens's are issued in a chain, where the next token is issued in
	// advance and linked to the previous token (and session).
	//
	// This ensure that in event of a race condition  of multiple HTTP calls,
	// and a token upgrade occurs. All HTTP calls would be upgraded to the
	// same succeding token.
	//
	///////////////////////////////////////////////////////////////////////////
	//#region login session management
	
	/**
	 * Checks if the current session is associated with the account
	 *
	 * @param  Session ID to validate
	 *
	 * @return TRUE if login ID belongs to this account
	 **/
	public boolean hasSession(String sessionID) {
		return sessionID != null && _oid.equals(mainTable.sessionLinkMap.getValue(sessionID));
	}
	
	/**
	 * List the various session ID's involved with this account
	 *
	 * @return  Set of login session ID's
	 **/
	public Set<String> getSessionSet() {
		return mainTable.sessionLinkMap.keySet(_oid);
	}
	
	/**
	 * Get and return the session information meta
	 *
	 * @param  Session ID to get
	 *
	 * @return  Information meta if session is valid
	 **/
	public Map<String, Object> getSessionInfo(String sessionID) {
		// Validate that session is legit
		if (!hasSession(sessionID)) {
			return null;
		}
		
		// Return the session information
		return mainTable.sessionInfoMap.getStringMap(sessionID, null);
	}
	
	/**
	 * Generate a new session with the provided meta information
	 *
	 * If no tokens are generated and issued in the next
	 * 30 seconds, the session will expire.
	 *
	 * Subseqently session expirary will be tag to
	 * the most recently generated token.
	 *
	 * The intended use case in the API, is for a token to be
	 * immediately issued after session via the API.
	 *
	 * Additionally info object is INTENTIONALLY NOT stored as a
	 * DataObject, for performance reasons.
	 *
	 * @param  Meta information map associated with the session,
	 *         a blank map is assumed if not provided.
	 *
	 * @return  The session ID used
	 **/
	public String newSession(Map<String, Object> info) {
		
		// Normalize the info object map
		if (info == null) {
			info = new HashMap<String, Object>();
		}
		
		// Set the session expirary time : 30 seconds (before tokens)
		long expireTime = ((System.currentTimeMillis()) / 1000L + mainTable.initSessionSetupLifespan) * 1000L;
		
		// Generate a base58 guid for session key
		String sessionID = GUID.base58();
		
		// As unlikely as it is, on GUID collision,
		// we do not want any session swarp EVER
		if (mainTable.sessionLinkMap.get(sessionID) != null) {
			throw new RuntimeException("GUID collision for sessionID : " + sessionID);
		}
		
		// Time to set it all up, with expire timestamp
		mainTable.sessionLinkMap.putWithExpiry(sessionID, _oid, expireTime);
		mainTable.sessionInfoMap.putWithExpiry(sessionID, ConvertJSON.fromMap(info), expireTime
			+ mainTable.sessionRaceConditionBuffer);
		
		// Return the session key
		return sessionID;
	}
	
	/**
	 * Revoke a session, associated to this account.
	 *
	 * This will also revoke all tokens associated to this session
	 *
	 * @param  SessionID to revoke
	 **/
	public void revokeSession(String sessionID) {
		// Validate the session belongs to this account !
		if (hasSession(sessionID)) {
			// Session ownership validated, time to revoke!
			
			// Revoke all tokens associated to this session
			revokeAllToken(sessionID);
			
			// Revoke the session info
			mainTable.sessionLinkMap.remove(sessionID);
			mainTable.sessionInfoMap.remove(sessionID);
		}
	}
	
	/**
	 * Revoke all sessions, associated to this account
	 **/
	public void revokeAllSession() {
		Set<String> sessions = getSessionSet();
		for (String oneSession : sessions) {
			revokeSession(oneSession);
		}
	}
	
	//#endregion login session management
	///////////////////////////////////////////////////////////////////////////
	//
	// Session.token management
	//
	///////////////////////////////////////////////////////////////////////////
	//#region login session.token management
	
	/**
	 * Checks if the current session token is associated with the account
	 *
	 * @param  Session ID to validate
	 * @param  Token ID to validate
	 *
	 * @return TRUE if login ID belongs to this account
	 **/
	public boolean hasToken(String sessionID, String tokenID) {
		return hasSession(sessionID) && sessionID.equals(mainTable.sessionTokenMap.getValue(tokenID));
	}
	
	/**
	 * Get the token set associated with the session and account
	 *
	 * @param   Session ID to fetch from
	 *
	 * @return  The list of token ID's currently associated to this session
	 *          null, if session is not valid.
	 **/
	public Set<String> getTokenSet(String sessionID) {
		if (hasSession(sessionID)) {
			return mainTable.sessionTokenMap.keySet(sessionID);
		}
		return new HashSet<String>();
	}
	
	/**
	 * Generate a new token, with a timeout
	 *
	 * Note that this token will update the session timeout,
	 * even if there was a longer session previously set.
	 *
	 * @param  Session ID to generate token from
	 * @param  The expire timestamp of the token
	 *
	 * @return  The tokenID generated, null on invalid session
	 **/
	public String newToken(String sessionID, long expireTime) {
		
		// Terminate if session is invalid
		if (!hasSession(sessionID)) {
			return null;
		}
		
		// Generate a base58 guid for session key
		String tokenID = GUID.base58();
		
		// Issue the token
		registerToken(sessionID, tokenID, GUID.base58(), expireTime);
		
		// Return the token
		return tokenID;
	}
	
	/**
	 * Internal function, used to issue a token ID to the session
	 * with the next token ID predefined
	 *
	 * @param  Session ID to generate token from
	 * @param  Token ID to setup
	 * @param  Next token ID
	 * @param  The expire timestamp of the token
	 *
	 * @return  The tokenID generated, null on invalid session
	 **/
	protected void registerToken(String sessionID, String tokenID, String nextTokenID,
		long expireTime) {
		// Current token check, does nothing if invalid
		if (hasToken(sessionID, tokenID)) {
			return;
		}
		
		// Check if token has already been registered
		String existingTokenSession = mainTable.sessionTokenMap.getString(tokenID, null);
		if (existingTokenSession != null) {
			// Check if token has valid session
			if (sessionID.equals(existingTokenSession)) {
				// Assume setup was already done, terminating
				return;
			} else {
				// Invalid next token, was issued to another session
				// EITHER a GUID collision occured, OR spoofing is being attempted
				throw new RuntimeException(
					"FATAL : Unable to register token previously registered to another session ID");
			}
		}
		
		// Renew every session!
		mainTable.sessionLinkMap.setExpiry(sessionID, expireTime
			+ mainTable.sessionRaceConditionBuffer);
		mainTable.sessionInfoMap.setExpiry(sessionID, expireTime
			+ mainTable.sessionRaceConditionBuffer * 2);
		
		// Register the token
		mainTable.sessionTokenMap.putWithExpiry(tokenID, sessionID, expireTime);
		mainTable.sessionNextTokenMap.putWithExpiry(tokenID, nextTokenID, expireTime);
	}
	
	/**
	 * Checks if the current session token is associated with the account
	 *
	 * @param  Session ID to validate
	 * @param  Token ID to revoke
	 *
	 * @return TRUE if login ID belongs to this account
	 **/
	public boolean revokeToken(String sessionID, String tokenID) {
		if (hasToken(sessionID, tokenID)) {
			mainTable.sessionTokenMap.remove(tokenID);
			return true;
		}
		return false;
	}
	
	/**
	 * Revokes all tokens associated to a session
	 *
	 * @param  Session ID to revoke
	 **/
	public void revokeAllToken(String sessionID) {
		Set<String> tokens = getTokenSet(sessionID);
		for (String oneToken : tokens) {
			revokeToken(sessionID, oneToken);
		}
	}
	
	/**
	 * Get token expiriry
	 *
	 * @param  Session ID to validate
	 * @param  Token ID to check
	 *
	 * @return  The token expiry timestamp
	 **/
	public long getTokenExpiry(String sessionID, String tokenID) {
		if (hasToken(sessionID, tokenID)) {
			return mainTable.sessionTokenMap.getExpiry(tokenID);
		}
		return -1;
	}
	
	/**
	 * Get token remaining lifespan
	 *
	 * @param  Session ID to validate
	 * @param  Token ID to check
	 *
	 * @return  The token remaining timespan, -1 means invalid token
	 **/
	public long getTokenLifespan(String sessionID, String tokenID) {
		// Get expiry timestamp
		long expiry = getTokenExpiry(sessionID, tokenID);
		
		// Invalid tokens are -1
		if (expiry <= -1) {
			return -1;
		}
		
		long lifespan = expiry - (System.currentTimeMillis()) / 1000L;
		if (lifespan < -1) {
			return -1;
		}
		return lifespan;
	}
	
	/**
	 * Internal function, used to get the next token given a session and current token.
	 * DOES NOT : Validate the next token if it exists
	 *
	 * @param  Session ID to validate
	 * @param  Token ID to check
	 *
	 * @return The next token
	 **/
	protected String getUncheckedNextToken(String sessionID, String tokenID) {
		if (hasToken(sessionID, tokenID)) {
			return mainTable.sessionNextTokenMap.getValue(tokenID);
		}
		return null;
	}
	
	/**
	 * Get the next token AFTER validating if it is issued
	 *
	 * @param  Session ID to validate
	 * @param  Token ID to check
	 *
	 * @return The next token ID
	 **/
	public String getNextToken(String sessionID, String tokenID) {
		String nextToken = getUncheckedNextToken(sessionID, tokenID);
		if (mainTable.sessionTokenMap.containsKey(nextToken)) {
			return nextToken;
		}
		return null;
	}
	
	/**
	 * Generate next token in line, with expiry
	 * Note that expiry is NOT set, if token was previously issued
	 *
	 * @param  Session ID to validate
	 * @param  Token ID to check
	 * @param  The expire timestamp of the token
	 *
	 * @return The next token ID
	 **/
	public String issueNextToken(String sessionID, String tokenID, long expireTime) {
		// Get NextToken, and returns (if previously issued)
		String nextToken = getUncheckedNextToken(sessionID, tokenID);
		// Terminate if nextToken is invalid
		if (nextToken == null) {
			return null;
		}
		
		// Issue next token
		registerToken(sessionID, nextToken, GUID.base58(), expireTime);
		// Return the next token, after its been issued
		return nextToken;
	}
	
	// /**
	//  * This method logs the details about login faailure for the user based on User ID
	//  **/
	// private void initializeLoginFailureAttempt() {
	// 	mainTable.loginThrottlingAttemptMap.put(this._oid(), 1);
	// 	int elapsedTime = ((int) (System.currentTimeMillis() / 1000)) + 2;
	// 	mainTable.loginThrottlingExpiryMap.put(this._oid(), elapsedTime);
	// }
	
	/**
	 * This method returns time left before next permitted login attempt for the user based on User ID
	 **/
	public int getNextLoginTimeAllowed() {
		long val = getExpiryTime();
		int allowedTime = (int) val - (int) (System.currentTimeMillis() / 1000);
		return allowedTime > 0 ? allowedTime : 0;
	}
	
	// /**
	//  * This method would be added in on next login failure for the user based on User ID
	//  **/
	// public long getTimeElapsedNextLogin() {
	// 	long elapsedValue = getExpiryTime();
	// 	return elapsedValue;
	// }
	
	/**
	 * This method would be increment the attempt counter and update the delay for the user
	 * to log in next
	 **/
	public void incrementNextAllowedLoginTime() {
		long attemptValue = mainTable.loginThrottlingAttemptMap.getAndIncrement(this._oid());
		int elapsedValue = (int) (System.currentTimeMillis() / 1000);
		elapsedValue += mainTable.calculateDelay.apply(this, attemptValue);
		mainTable.loginThrottlingExpiryMap.weakCompareAndSet(this._oid(), getExpiryTime(),
			(long) elapsedValue);
	}
	
	private long getAttempts() {
		return mainTable.loginThrottlingAttemptMap.getLong(this._oid(), 0l);
	}
	
	private long getExpiryTime() {
		return mainTable.loginThrottlingExpiryMap.getLong(this._oid(), 0l);
	}
	
	//#region login session.token management
}
