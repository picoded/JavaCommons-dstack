package picoded.module.account;

import java.util.*;
import java.util.function.BiFunction;

import picoded.dstack.*;
import picoded.module.*;
import picoded.core.conv.*;
import picoded.core.struct.*;
import picoded.core.security.NxtCrypt;
import picoded.core.struct.template.UnsupportedDefaultMap;

/**
 * AccountTable, used to facilitate the following
 * 
 * - account creation
 * - authentication  
 * - login of websessions
 * - password reset tokens
 * 
 * @TODO : in future
 * - account api key support
 *
 **/
public class AccountTable extends AccountTableBasic {
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Constructor setup : Setup the actual tables, with the various names
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Setup with the given stack and name prefix for data structures
	 **/
	public AccountTable(CommonStack inStack, String inName) {
		super(inStack, inName);
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Reset token configuration
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * New session lifespan without token
	 **/
	protected int resetTokenLifetime = 3600 * 24; // 24 hour in seconds
	
	/**
	 * Timeframe for issuing a new token, in event that two tokens are issues back to back
	 * 
	 * If token was previously issued within this time frame, the same token will be used for reset.
	 * If token is after the given time frame, a new token is issued, revoking the previous token.
	 **/
	protected int resetTokenReissueLifetime = resetTokenLifetime / 2;
	
	/**
	 * Reset token string lenngth
	 * use 22 so is interchangeable with base58 guid
	 **/
	protected int resetTokenStringLength = 22;
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Reset token function
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Issue a token for use with the password reset API
	 * This should be transmitted to the user account via email.
	 * 
	 * @param  accountObject to perform the restart process
	 * 
	 * @return  reset password token
	 */
	public String issueResetToken(AccountObject accountObject) {
		// Get the account ID 
		String accountID = accountObject._oid();
		
		// Get any existing token (if found)
		KeyValue existingToken = accountPasswordResetTokenMap.get(accountID);
		
		// Check for existing token
		//----------------------------------------------
		
		if (existingToken != null) {
			// Check if reset token is within lifetime thrashold
			// if so - it just reissues it.
			long lifespan = existingToken.getLifespan();
			if (lifespan >= (resetTokenReissueLifetime * 1000)) {
				return existingToken.getValue();
			}
		}
		
		// At this point : a new token must be issued
		//----------------------------------------------
		
		// Issue a new token
		String resetToken = NxtCrypt.randomString(resetTokenStringLength);
		
		// Store it with accountID in both direction - allowing easy cross referencing
		// side note : depends on pure GUID collision prevention
		// Multiply by 1000 to convert 24 hours into milliseconds
		accountPasswordResetTokenMap
			.putWithLifespan(accountID, resetToken, resetTokenLifetime * 1000);
		accountPasswordResetTokenMap
			.putWithLifespan(resetToken, accountID, resetTokenLifetime * 1000);
		
		// Return the token
		return resetToken;
	}
	
	/**
	 * Given the password token, and the new password
	 * Reset the account password. Returns true on success
	 * 
	 * @param  token for reset
	 * @param  password to apply for reset
	 * 
	 * @return  true on succesful password reset
	 */
	public boolean applyResetToken(String token, String password) {
		
		// Quick fail fast validations, and getting the accountID
		//--------------------------------------------------------
		
		// Fast failing on invalid param
		if (token == null || token.isEmpty() || password == null || password.isEmpty()) {
			return false;
		}
		
		// Get the supposed accountID from the token
		String accountID = accountPasswordResetTokenMap.getValue(token);
		
		// Failed to get a valid accountID
		if (accountID == null) {
			return false;
		}
		
		// Cross validating with the additional key record
		// for accountID to token mapping
		//
		// This guard against invalid password reset on tokenID 
		// generation of collision IDs
		//
		// (theorectically insanely unlikely possiblility on low entropy, 
		// identical VM image on boot, or the sun exploding, or time traveling aliens)
		//--------------------------------------------------------
		
		KeyValue accountToken = accountPasswordResetTokenMap.get(accountID);
		
		// Expirary race condition (null) handling
		if (accountToken == null) {
			return false;
		}
		
		if (!token.equals(accountToken.getValue())) {
			// Invalid match, since this is an exceedingly rare event - throw exception
			throw new RuntimeException(
				"Account Token Mismatch - Something is really wrong here, contact help ASAP! (Seriously)");
		}
		
		// Since KeyValue ALREADY handles automated expirary
		// having a valid token info here means a valid account
		//--------------------------------------------------------
		
		// Get the account object linked to the token
		AccountObject accountToUpdate = this.get(accountID);
		
		// Invalid token / missing account object, return false
		if (accountToUpdate == null) {
			return false;
		}
		
		// Invalidate password tokens
		accountPasswordResetTokenMap().remove(accountID);
		accountPasswordResetTokenMap().remove(accountToken.getKey());
		
		// Apply password reset accordingly
		accountToUpdate.setPassword(password);
		accountToUpdate.resetLoginThrottle();
		return true;
	}
	
}