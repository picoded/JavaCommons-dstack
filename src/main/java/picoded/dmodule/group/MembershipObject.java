package picoded.dmodule.group;

import java.util.*;

import picoded.dstack.*;
import picoded.dstack.core.*;
import picoded.core.conv.*;
import picoded.core.struct.*;

/**
 * Represents a single group / member relationship.
 **/
public class MembershipObject
// extends Core_DataObject 
{
	
	// ///////////////////////////////////////////////////////////////////////////
	// //
	// // Constructor and setup
	// //
	// ///////////////////////////////////////////////////////////////////////////
	// //#region constructor and setup
	
	// /**
	//  * The original account table
	//  **/
	// protected MembershipTable main = null;
	
	// /**
	//  * [INTERNAL USE ONLY]
	//  *
	//  * Cosntructor setup, using an account table,
	//  * and the account GUID
	//  **/
	// protected MembershipObject(MembershipTable mainTable, String inOID) {
	// 	// Inherit all the default data table methods
	// 	super((Core_DataTable) (mainTable.membershipTable), inOID);
	// 	main = mainTable;
	// }
	
	// /**
	//  * Put and set its delta value, set null is considered "remove"
	//  *
	//  * @param  key to use
	//  * @param  Value to store, does conversion to numeric if possible
	//  *
	//  * @return The previous value
	//  **/
	// @Override
	// public Object put(String key, Object value) {
	// 	// Safety check for reserved keywords
	// 	if(key.equalsIgnoreCase("groupid") || key.equalsIgnoreCase("memberid")) {
	// 		// Check for non-null value, which is after first time setup.
	// 		Object originalValue = get(key.toLowerCase());
	// 		// Original value exist, throws an exception on value change
	// 		// else skips and "does nothing"
	// 		if( originalValue != null ) {
	// 			// Orginal value matches target value, skip put call
	// 			if( originalValue.toString() == value.toString() ) {
	// 				return null;
	// 			}
	// 			// Value change detected : throw an exception
	// 			throw new RuntimeException("Unable to change groupid/memberid - these are protected values");
	// 		}
	// 	}
	// 	// "Super" call
	// 	return super.put(key, value);
	// }
	
}
