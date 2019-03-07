package picoded.module.account;

import java.util.*;
import java.util.function.BiFunction;

import picoded.dstack.*;
import picoded.module.*;
import picoded.core.conv.*;
import picoded.core.struct.*;
import picoded.core.struct.template.UnsupportedDefaultMap;

/**
 * AccountTableBasic, used to facilitate basic acccount 
 * creation, authentication and login of websessions.
 *
 * Any refences to the persona game, is completely coincidental !
 * (PS: old joke, the original name for this class was PersonaTable)
 **/
public class AccountTableBasic extends AccountTableCore {
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Constructor setup : Setup the actual tables, with the various names
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Setup with the given stack and name prefix for data structures
	 **/
	public AccountTableBasic(CommonStack inStack, String inName) {
		super(inStack, inName);
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Servlet login handling
	//
	// These features depends on the following packages
	//
	// import javax.servlet.ServletException;
	// import javax.servlet.http.HttpServletRequest;
	// import javax.servlet.http.HttpServletResponse;
	// import javax.servlet.http.Cookie;
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Internal call to store the actual cookie and the respective values
	 *
	 * @param   HTTP request to read server settings from
	 * @param   HTTP Response to write into
	 * @param   Session ID to store
	 * @param   Token ID to store
	 * @param   Remember me settings
	 * @param   The cookie lifetime, 0 deletes the cookie, else its ignore if remember me is false
	 * @param   The cookie expire timestamp
	 **/
	protected boolean storeCookiesInsideTheCookieJar(javax.servlet.http.HttpServletRequest request,
		javax.servlet.http.HttpServletResponse response, String sessionID, String tokenID,
		boolean rememberMe, int lifeTime, long expireTime) {
		
		// instant failure without response object
		if (response == null) {
			return false;
		}
		
		// Setup the cookie jar
		int noOfCookies = 4;
		javax.servlet.http.Cookie cookieJar[] = new javax.servlet.http.Cookie[noOfCookies];
		
		// Store session and token cookies
		cookieJar[0] = new javax.servlet.http.Cookie(cookiePrefix + "ses", sessionID);
		cookieJar[1] = new javax.servlet.http.Cookie(cookiePrefix + "tok", tokenID);
		
		// Remember me configuration
		// Should this be handled usign server side storage data?
		// If its not a valid security threat, this should be ok right?
		if (rememberMe) {
			cookieJar[2] = new javax.servlet.http.Cookie(cookiePrefix + "rmb", "1");
		} else {
			cookieJar[2] = new javax.servlet.http.Cookie(cookiePrefix + "rmb", "0");
		}
		
		// The cookie "exp"-iry store the other cookies (Rmbr, user, Nonc etc.) expiry life time in seconds.
		// This cookie value is used in JS (checkLogoutTime.js) for validating the login expiry time
		// and show a message to user accordingly.
		//
		// Note that this cookie IGNORES isHttpOnly setting
		cookieJar[3] = new javax.servlet.http.Cookie(cookiePrefix + "exp", String.valueOf(expireTime));
		
		// Storing the cookie jar with the browser
		for (int a = 0; a < noOfCookies; ++a) {
			
			/**
			 * Cookie Path is required for cross AJAX / domain requests,
			 * This is taken from the request settings, if not defined
			 **/
			String cPath = cookiePath;
			if (cPath == null) {
				if (request.getContextPath() == null || request.getContextPath().isEmpty()) {
					cPath = "/";
				} else {
					cPath = request.getContextPath();
				}
			}
			cookieJar[a].setPath(cPath);
			
			// If remember me is configured
			if (rememberMe || lifeTime == 0) {
				cookieJar[a].setMaxAge(lifeTime);
			} else {
				cookieJar[a].setMaxAge(-1);
			}
			
			// Set isHttpOnly flag, to prevent JS based session attacks
			// this is ignored for the expire timestamp field (index = 3)
			if (isHttpOnly && a != 3) {
				cookieJar[a].setHttpOnly(isHttpOnly);
			}
			
			// Set it to be https strict if relevent
			if (isSecureOnly) {
				cookieJar[a].setSecure(isSecureOnly);
			}
			
			// Set a strict cookie domain
			if (cookieDomain != null && cookieDomain.length() > 0) {
				cookieJar[a].setDomain(cookieDomain);
			}
			
			// Actually inserts the cookie
			response.addCookie(cookieJar[a]);
		}
		
		// Valid
		return true;
	}
	
	/**
	 * Performs the login to a user (handles the respective session tokens) and set the cookies for the response.
	 *
	 * As this does the login without the actual password authentication steps.
	 * Unless you are creating a custom login intergration. DO NOT USE this, and use loginUser instead, with the
	 * relevent username and password.
	 *
	 * The cookie is configured to store the following information under the "cookiePrefix" (default Account_)
	 * + Session ID
	 * + Token ID
	 * + Expiriry Timestamp (for JS to read)
	 * + Remember Me flag
	 *
	 * @param  Account object used
	 * @param  The http request to read
	 * @param  The http response to write into
	 * @param  Indicator for "remember me" functionality
	 * @param  Session information map to use, useful to set custom flags, can be null
	 *
	 * @return  Login success or failure
	 **/
	public boolean bypassSecurityChecksAndPerformNewAccountLogin(AccountObject ao,
		javax.servlet.http.HttpServletRequest request,
		javax.servlet.http.HttpServletResponse response, boolean rememberMe,
		Map<String, Object> sessionInfo) {
		// Null check
		if (request == null) {
			return false;
		}
		
		// Prepare the vars
		//-----------------------------------------------------
		String aoid = ao._oid();
		
		// Detirmine the login lifetime
		int lifeTime = getLifeTime(rememberMe);
		long expireTime = ((System.currentTimeMillis()) / 1000L + lifeTime) * 1000L;
		
		// Session info handling
		//-----------------------------------------------------
		
		// Prepare the session info
		if (sessionInfo == null) {
			sessionInfo = new HashMap<String, Object>();
		}
		
		// Lets do some USER_AGENT sniffing
		sessionInfo.put("USER_AGENT", request.getHeader("USER_AGENT"));
		
		// @TODO : Conisder sniffing additional info such as IP address
		
		// Generate the session and tokens
		//-----------------------------------------------------
		
		String sessionID = ao.newSession(sessionInfo);
		String tokenID = ao.newToken(sessionID, expireTime);
		
		// Store the cookies, and end
		//-----------------------------------------------------
		return storeCookiesInsideTheCookieJar(request, response, sessionID, tokenID, rememberMe,
			lifeTime, expireTime);
	}
	
	/**
	 * Logout any existing users
	 *
	 * @param  The http request to read
	 * @param  The http response to write into
	 *
	 * @return  Logout success or failure
	 **/
	public boolean logoutAccount(javax.servlet.http.HttpServletRequest request,
		javax.servlet.http.HttpServletResponse response) {
		if (response == null) {
			return false;
		}
		
		return storeCookiesInsideTheCookieJar(request, response, "-", "-", false, 0, 0);
	}
	
	/**
	 * Validates the user retur true/false, with an update response cookie / token if needed
	 *
	 * NOTE: login session renewal will not be performed on request url containing the keyword "logout"
	 *
	 * @param   http servlet request
	 * @param   http servlet response (optional)
	 *
	 * @return  Valid logged in account objec
	 **/
	public AccountObject getRequestUser(javax.servlet.http.HttpServletRequest request,
		javax.servlet.http.HttpServletResponse response) {
		// Null check
		if (request == null) {
			return null;
		}
		
		javax.servlet.http.Cookie[] cookieJar = request.getCookies();
		if (cookieJar == null) {
			return null;
		}
		
		// Gets the existing cookie settings
		//----------------------------------------------------------
		String sessionID = null;
		String tokenID = null;
		boolean rememberMe = false;
		
		for (javax.servlet.http.Cookie crumbs : cookieJar) {
			String crumbsFlavour = crumbs.getName();
			
			if (crumbsFlavour == null) {
				continue;
			} else if (crumbsFlavour.equals(cookiePrefix + "ses")) {
				sessionID = crumbs.getValue();
			} else if (crumbsFlavour.equals(cookiePrefix + "tok")) {
				tokenID = crumbs.getValue();
			} else if (crumbsFlavour.equals(cookiePrefix + "rmb")) {
				rememberMe = "1".equals(crumbs.getValue());
			}
		}
		
		// Time to validate the cookie settings
		//----------------------------------------------------------
		
		// Check if a session id and token id was provided
		// in a valid format
		if (sessionID == null || tokenID == null || sessionID.length() < 22 || tokenID.length() < 22) {
			return null;
		}
		
		// If an invalid session / token ID is provided, assume logout
		AccountObject ret = getFromSessionID(sessionID);
		
		// Session ID fails to fetch an account object
		if (ret == null) {
			logoutAccount(request, response);
			return null;
		}
		
		// Get the token lifespan, not that this also
		// check for invalid session and token
		long tokenLifespan = ret.getTokenLifespan(sessionID, tokenID);
		if (tokenLifespan <= 0) {
			// Does logout on invalid token
			logoutAccount(request, response);
			return null;
		}
		
		// From this point onwards, the session is valid. Now it performs checks for the renewal process
		// Does nothing if response object is not given
		//---------------------------------------------------------------------------------------------------
		
		// Do not set cookies if it is logout request, and return the result.
		// This is to prevent session renewal and revoking from happening simultainously
		// creating unexpected behaviour
		//
		// @TODO : Consider a more fixed pattern?
		if (request.getPathInfo() != null && request.getPathInfo().indexOf("logout") > 0) {
			return ret;
		}
		
		if (response != null) {
			
			// Renewal checking
			boolean needRenewal = false;
			if (rememberMe) {
				if (tokenLifespan < rememberMeRenewal) { // needs renewal (perform it!)
					needRenewal = true;
				}
			} else {
				if (tokenLifespan < loginRenewal) { // needs renewal (perform it!)
					needRenewal = true;
				}
			}
			
			// Actual renewal process
			if (needRenewal) {
				// Detirmine the renewed login lifetime and expirary to set (if new issued token)
				long expireTime = ((System.currentTimeMillis()) / 1000L + getLifeTime(rememberMe)) * 1000L;
				
				// Issue the next token
				String nextTokenID = ret.issueNextToken(sessionID, tokenID, expireTime);
				
				// Get the actual expiry of the next token (if it was previously issued)
				expireTime = ret.getTokenExpiry(sessionID, nextTokenID);
				
				// If nextTokenID and expireTime fails, assume login failure
				if (nextTokenID == null || expireTime < 0) {
					logoutAccount(request, response);
					return null;
				}
				
				// Get lifespan
				long lifespan = expireTime - (System.currentTimeMillis());
				
				// Setup the next token
				storeCookiesInsideTheCookieJar(request, response, sessionID, nextTokenID, rememberMe,
					(int) lifespan, expireTime);
			}
		}
		
		// Return the validated account object
		//---------------------------------------------------------------------------------------------------
		return ret;
	}
	
	/**
	 * Login the user if the given values are valid, and return its account object
	 *
	 * @param   http servlet request
	 * @param   http servlet response
	 * @param   Account object to perform login
	 * @param   Raw password to validate
	 * @param   Remember me boolean (if set)
	 *
	 * @return  The logged in account object
	 **/
	public AccountObject loginAccount(javax.servlet.http.HttpServletRequest request,
		javax.servlet.http.HttpServletResponse response, AccountObject accountObj,
		String rawPassword, boolean rememberMe) {
		if (accountObj != null && accountObj.validatePassword(rawPassword)) {
			bypassSecurityChecksAndPerformNewAccountLogin(accountObj, request, response, rememberMe,
				null);
			return accountObj;
		}
		return null;
	}
	
	/**
	 * Login the user if the given values are valid, and return its account object
	 *
	 * @param   http servlet request
	 * @param   http servlet response
	 * @param   Account nice login ID (normally email)
	 * @param   Raw password to validate
	 * @param   Remember me boolean (if set)
	 *
	 * @return  The logged in account object
	 **/
	public AccountObject loginAccount(javax.servlet.http.HttpServletRequest request,
		javax.servlet.http.HttpServletResponse response, String nicename, String rawPassword,
		boolean rememberMe) {
		return loginAccount(request, response, getFromLoginName(nicename), rawPassword, rememberMe);
	}
	
}
