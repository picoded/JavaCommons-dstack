package picoded.module.group;

import java.util.*;
import java.util.function.BiFunction;

import picoded.dstack.*;
import picoded.module.*;
import picoded.core.conv.*;
import picoded.core.struct.*;
import picoded.core.struct.query.Query;

/**
 * MembershipTable used to provide group ownership based functionality,
 * against existing data table compatible backend.
 **/
public abstract class MembershipTable extends ModuleStructure {
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Underlying data structures
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Handles the storage of group data.
	 *
	 * Note: Consider this the "PRIMARY TABLE"
	 *
	 * DataObjectMap<GroupOID, DataObject>
	 **/
	protected DataObjectMap groupTable = null;
	
	/**
	 * Member DataObjectMap, to link. Note that this is designed,
	 * intentionally to work with DataObjectMap compatible interface.
	 *
	 * DataObjectMap<AccountOID, DataObject>
	 **/
	protected DataObjectMap memberTable = null;
	
	/**
	 * Handles the storage of group to partipant link.
	 *
	 * DataObjectMap<MembershipOID, DataObject>
	 **/
	protected DataObjectMap membershipTable = null;
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Constructor setup : Setup the actual tables, with the various names
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Blank constructor, used for more custom extensions
	 **/
	public MembershipTable() {
		super();
	}
	
	/**
	 * MembershipTable constructor using DataObjectMap
	 *
	 * @param  inGroup         group table to build / extend from
	 * @param  inMember        partipant table to build / extend from
	 * @param  membership      primary membership table to link the group / partipant table
	 **/
	public MembershipTable(DataObjectMap inGroup, DataObjectMap inMember, DataObjectMap membership) {
		super();
		groupTable = inGroup;
		memberTable = inMember;
		membershipTable = membership;
		// Setup the internal structure list
		internalStructureList = internalStructureList();
	}
	
	//----------------------------------------------------------------
	//
	//  Internal CommonStructure management
	//
	//----------------------------------------------------------------
	
	/**
	 * Setup the list of local CommonStructure's
	 * this is used internally by setup/destroy/maintenance
	 **/
	protected List<CommonStructure> internalStructureList() {
		// Check for missing items
		if (groupTable == null || memberTable == null || membershipTable == null) {
			throw new RuntimeException("Missing group/member/membership table");
		}
		// Return it as a list
		return Arrays.asList(groupTable, memberTable, membershipTable);
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Various sub table access
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Group table - that represent the group ownership
	 * @return DataObjectMap which represents the group table
	 */
	public DataObjectMap groupTable() {
		return groupTable;
	}

	/**
	 * Member table - that represent the respective members
	 * @return DataObjectMap which represents the Member table
	 */
	public DataObjectMap memberTable() {
		return memberTable;
	}

	/**
	 * Membership table - that represent the respective relationships between group and member
	 * @return DataObjectMap which represents the membership table
	 */
	public DataObjectMap membershipTable() {
		return membershipTable;
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Membership based utility functions
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Validate group and member ID, returns false on failure
	 *
	 * @param  groupID   group id to fetch from
	 * @param  memberID  member id to fetch from
	 * 
	 * @return true, if membership is validated
	 */
	protected boolean validateMembership(String groupID, String memberID) {
		// Perform cleanup, if the parent group object was removed
		if (!groupTable.containsKey(groupID)) {
			cleanupMembership(groupID, null);
			return false;
		}
		// Perform cleanup, if the member object was removed
		if (!memberTable.containsKey(memberID)) {
			cleanupMembership(null, memberID);
			return false;
		}
		return true;
	}
	
	/**
	 * Clean up membership objects, if the parent objects orphaned them
	 *
	 * @param  groupID   group id to remove, effectively ignored if null
	 * @param  memberID  member id to remove, effectively ignored if null
	 */
	protected void cleanupMembership(String groupID, String memberID) {

		// The id's needed to perform cleanup
		String[] ids = null;

		// Different query behaviours according to the parmameters provided
		if( groupID != null && memberID != null ) {
			// Both params is provided, does a larger query
			ids = membershipTable.query_id("_groupid=? OR _memberid=?", new Object[] { groupID,
				memberID }, null);
		} else if( groupID != null ) {
			// Only group ID is provided
			ids = membershipTable.query_id("_groupid=?", new Object[] { groupID }, null);
		} else if( memberID != null ) {
			// Only member ID is provided
			ids = membershipTable.query_id("_memberid=?", new Object[] { memberid }, null);
		} else {
			// Does nothing (no valid conditions)
			return;
		}
		
		if (ids != null && ids.length > 0) {
			// Detecting more then one object match, remove obseleted items
			for (int i = 0; i < ids.length; ++i) {
				membershipTable.remove(ids[i]);
			}
		}
	}
	
	/**
	 * Gets and return the membership object, only if it exists.
	 *
	 * @param  groupID   group id to fetch from
	 * @param  memberID  member id to fetch from
	 *
	 * @return  membership object ID
	 */
	protected String getMembershipID(String groupID, String memberID) {
		// Basic id validation
		validateMembership(groupID, memberID);
		
		// @CONSIDER : Adding key-value map caching layer to optimize group/memberid to membership-id
		// @CONSIDER : Collision removal checking by timestamp, where oldest wins
		String[] ids = membershipTable.query_id("_groupid=? AND _memberid=?", new Object[] { groupID,
			memberID }, "DESC _oid");
		if (ids != null && ids.length > 0) {
			// Detecting more then one object match, remove collision
			if (ids.length > 1) {
				for (int i = 1; i < ids.length; ++i) {
					membershipTable.remove(ids[i]);
				}
			}
			return ids[0];
		}
		return null;
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Basic add / get / remove membership
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Gets and return the membership object, only if it exists.
	 *
	 * @param  groupID   group id to fetch from
	 * @param  memberID  member id to fetch from
	 *
	 * @return  membership object (if found)
	 */
	public DataObject getMembership(String groupID, String memberID) {
		String id = getMembershipID(groupID, memberID);
		if (id != null) {
			return membershipTable.get(id);
		}
		return null;
	}
	
	/**
	 * Add membership object, only if doesnt previously exists
	 * Else fetches the membership object.
	 *
	 * @param  groupID   group id to fetch from
	 * @param  memberID  member id to fetch from
	 *
	 * @return  membership object (if created / existed)
	 */
	public DataObject addMembership(String groupID, String memberID) {
		String id = getMembershipID(groupID, memberID);
		// Create and save a new membership object
		// if it does not exists
		if (id == null) {
			DataObject obj = membershipTable.newEntry();

			// Relationship mapping
			obj.put("_groupid", groupID);
			obj.put("_memberid", memberID);
			obj.saveAll();
		}
		// Get the newly saved membership object
		return getMembership(groupID, memberID);
	}
	
	/**
	 * Remove membership object, only if doesnt previously exists
	 * Else fetches the membership object.
	 *
	 * @param  groupID   group id to fetch from
	 * @param  memberID  member id to fetch from
	 *
	 * @return  membership object (if created / existed)
	 */
	public void removeMembership(String groupID, String memberID) {
		String id = getMembershipID(groupID, memberID);
		if (id != null) {
			membershipTable.remove(id);
		}
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Membership relation lookup
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Get and list all the related membership objects, given the memberID
	 * 
	 * @param memberID to lookup for
	 * 
	 * @return list of membership objects
	 */
	private List<DataObject> listMembership_fromMemberID(String memberID) {
		return Arrays.asList( membershipTable.query("_memberid = ?", new Object[] { memberID }) );
	}

	/**
	 * Get and list all the related membership objects, given the memberID,
	 * filtered by the given query
	 * 
	 * @param memberID to lookup for
	 * 
	 * @return list of membership objects
	 */
	private List<DataObject> listMembership_fromMemberID(String memberID, String membershipQuery, Object[] queryArgs) {
		// Get list of membership objects via memberID
		List<DataObject> raw = listMembership_fromMemberID(memberID);

		// Return as it is
		if( membershipQuery == null ) {
			return raw;
		}

		// Apply additional query
		Query query = Query.build(membershipQuery, queryArgs);
		return query.search(raw);
	}

	
	




















	// 	///////////////////////////////////////////////////////////////////////////
	// 	//
	// 	// basic Membership listing
	// 	//
	// 	///////////////////////////////////////////////////////////////////////////
	
	// 	/**
	// 	 * Gets the list of membership given a group
	// 	 *
	// 	 * @param  groupID   group id to fetch from
	// 	 *
	// 	 * @return  list of relevent members
	// 	 */
	// 	public MembershipObject[] listMembership_fromGroup(String groupID) {
	// 		return castFromDataObject ( groupTable.query("_groupid=?", new Object[] { groupID }) );
	// 	}
	
	// 	/**
	// 	 * Gets the list of membership given a member
	// 	 *
	// 	 * @param  memberID  member id to fetch from
	// 	 *
	// 	 * @return  list of relevent members
	// 	 */
	// 	public MembershipObject[] listMembership_fromMember(String memberID) {
	// 		return castFromDataObject ( groupTable.query("_memberid=?", new Object[] { memberID }) );
	// 	}
	
	// 	///////////////////////////////////////////////////////////////////////////
	// 	//
	// 	// DataObjectMap based query
	// 	//
	// 	// @TODO : Refactor this out to a core.struct.query.AbstractMapMapQuery
	// 	//
	// 	///////////////////////////////////////////////////////////////////////////
	
	// 	// ///////////////////////////////////////////////////////////////////////////
	// 	// //
	// 	// // Universal object listing =/
	// 	// //
	// 	// ///////////////////////////////////////////////////////////////////////////
	
	// 	// /**
	// 	//  * Does a rather complex multi-query, across group, member, and membership.
	// 	//  * And return the chosen object type.
	// 	//  *
	// 	//  * For query / args pairs that are null, they are effectively wildcards.
	// 	//  *
	// 	//  * @param  groupQuery        group based query
	// 	//  * @param  groupArgs         group based query args
	// 	//  * @param  memberQuery       member based query
	// 	//  * @param  memberArgs        member based query args
	// 	//  * @param  membershipQuery   membership based query
	// 	//  * @param  membershipArgs    membership based query args
	// 	//  *
	// 	//  * @return list of memberships, after all the various queries
	// 	//  */
	// 	// public List<MembershipObject> multiQuery(
	// 	// 	String groupQuery,      Object[] groupArgs,
	// 	// 	String memberQuery,     Object[] memberArgs,
	// 	// 	String membershipQuery, Object[] membershipArgs
	// 	// ) {
	// 	// 	// Note that the query route changes
	// 	// 	// According to the parameters provided
	// 	// 	// Under currently best guess assumptions
	// 	// 	//
	// 	// 	// @CONSIDER : Optimizing the query based on real usage
	// 	// 	//             or alternatively with a hint parameter
	// 	// 	if( groupQuery != null && groupArgs != null ) {
	// 	// 		// If group query is provided, query will be executed
	// 	// 		// in the following sequence, skipping if needed.
	// 	// 		//
	// 	// 		// 1. groupQuery
	// 	// 		// 2. membershipQuery
	// 	// 		// 3. memberQuery
	// 	// 	}
	// 	// 	// DataObject[] groupList = null;
	// 	// 	// DataObject[] memberList = null;
	// 	// 	// // Build using group list first, if query is given
	// 	// 	// if( groupQuery != null && groupArgs != null ) {
	// 	// 	// 	groupList = groupTable.query(groupQuery, groupArgs);
	// 	// 	// }
	// 	// 	// // Build from member list
	// 	// 	// if( memberList != null && memberArgs != null ) {
	// 	// 	// 	// Group list is null, so use member query directly
	// 	// 	// 	if( groupList == null ) {
	// 	// 	// 	}
	// 	// 	// }
	// 	// 	return null;
	// 	// }
}
