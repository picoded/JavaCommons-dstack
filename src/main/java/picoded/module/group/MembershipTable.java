package picoded.module.group;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import picoded.dstack.*;
import picoded.module.*;
import picoded.core.conv.*;
import picoded.core.struct.*;
import picoded.core.struct.query.Query;

/**
 * MembershipTable used to provide group ownership based functionality,
 * against existing data table compatible backend.
 * 
 * Note that membership and relationship is used interchangably.
 * 
 * In general for parameters and internal function we use the term
 * relationship, to avoid confusion of the word "member" with "membership"
 **/
public class MembershipTable extends ModuleStructure {
	
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
	protected DataObjectMap relationshipTable = null;
	
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
	 * @param  inRelationship    primary relationship table to link the group / partipant table
	 **/
	public MembershipTable(DataObjectMap inGroup, DataObjectMap inMember,
		DataObjectMap inRelationship) {
		super();
		groupTable = inGroup;
		memberTable = inMember;
		relationshipTable = inRelationship;
		// Setup the internal structure list
		internalStructureList = internalStructureList();
	}
	
	/**
	 * Initialize the various internal data structures,
	 * used by account from the stack.
	 **/
	protected List<CommonStructure> setupInternalStructureList() {
		// Return it as a list
		return Arrays.asList( //
			groupTable, memberTable, relationshipTable //
			);
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
		if (groupTable == null || memberTable == null || relationshipTable == null) {
			throw new RuntimeException("Missing group/member/relationship table");
		}
		// Return it as a list
		return Arrays.asList(groupTable, memberTable, relationshipTable);
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
	 * @return DataObjectMap which represents the relationship table
	 */
	public DataObjectMap relationshipTable() {
		return relationshipTable;
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
	 * @return true, if relationship is validated
	 */
	protected boolean validateIDWithoutCleanup(String groupID, String memberID) {
		
		// @TODO: Create another validation ID function that will perform the cleanup action
		// For now, the scope is to just validate the IDs
		
		// Validate that the IDs exists in the group and member tables
		if (!groupTable.containsKey(groupID) || !memberTable.containsKey(memberID)) {
			return false;
		}
		return true;
	}
	
	/**
	 * Clean up relationship objects, if the parent objects orphaned them
	 *
	 * @param  groupID   group id to remove, effectively ignored if null
	 * @param  memberID  member id to remove, effectively ignored if null
	 */
	protected void cleanupMembership(String groupID, String memberID) {
		
		// The id's needed to perform cleanup
		String[] ids = null;
		
		// Different query behaviours according to the parmameters provided
		if (groupID != null && memberID != null) {
			// Both params is provided, does a larger query
			ids = relationshipTable.query_id("_groupID=? OR _memberID=?", new Object[] { groupID,
				memberID }, null);
		} else if (groupID != null) {
			// Only group ID is provided
			ids = relationshipTable.query_id("_groupID=?", new Object[] { groupID }, null);
		} else if (memberID != null) {
			// Only member ID is provided
			ids = relationshipTable.query_id("_memberID=?", new Object[] { memberID }, null);
		} else {
			// Does nothing (no valid conditions)
			return;
		}
		
		if (ids != null && ids.length > 0) {
			// Detecting more then one object match, remove obseleted items
			for (int i = 0; i < ids.length; ++i) {
				relationshipTable.remove(ids[i]);
			}
		}
	}
	
	/**
	 * Gets and return the relationship object, only if it exists.
	 *
	 * @param  groupID   group id to fetch from
	 * @param  memberID  member id to fetch from
	 *
	 * @return  relationship object ID
	 */
	protected String getMembershipID(String groupID, String memberID) {
		// Basic id validation
		if (!validateIDWithoutCleanup(groupID, memberID)) {
			throw new IllegalArgumentException("Either the Group ID or Member ID does not exist");
		}
		
		// @CONSIDER : Adding key-value map caching layer to optimize group/memberID to relationship-id
		// @CONSIDER : Collision removal checking by timestamp, where oldest wins
		String[] ids = relationshipTable.query_id("_groupID=? AND _memberID=?", new Object[] {
			groupID, memberID });
		if (ids != null && ids.length > 0) {
			// Detecting more then one object match, remove collision
			if (ids.length > 1) {
				for (int i = 1; i < ids.length; ++i) {
					relationshipTable.remove(ids[i]);
				}
			}
			return ids[0];
		}
		return null;
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Basic add / get / remove relationship
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Gets and return the relationship object, only if it exists.
	 *
	 * @param  groupID   group id to fetch from
	 * @param  memberID  member id to fetch from
	 *
	 * @return  relationship object (if found)
	 */
	public DataObject getMembership(String groupID, String memberID) {
		String id = getMembershipID(groupID, memberID);
		if (id != null) {
			return relationshipTable.get(id);
		}
		return null;
	}
	
	/**
	 * Add relationship object, only if doesnt previously exists
	 * Else fetches the relationship object.
	 *
	 * @param  groupID   group id to fetch from
	 * @param  memberID  member id to fetch from
	 *
	 * @return  relationship object (if created / existed)
	 */
	public DataObject addMembership(String groupID, String memberID) {
		
		// Normalize all null parameters to be empty strings
		groupID = (groupID == null) ? "" : groupID;
		memberID = (memberID == null) ? "" : memberID;
		
		if (memberID.isEmpty() || groupID.isEmpty()) {
			throw new IllegalArgumentException("Both Group ID and Member ID must not be null/empty");
		}
		
		// Get the membership obj
		DataObject ret = getMembership(groupID, memberID);
		
		// Exit early if found
		if (ret != null) {
			return ret;
		}
		
		// Create and save a new relationship object
		DataObject obj = relationshipTable.newEntry();
		
		// Relationship mapping
		obj.put("_groupID", groupID);
		obj.put("_memberID", memberID);
		obj.saveAll();
		
		return obj;
	}
	
	/**
	 * Remove relationship object, only if doesnt previously exists
	 * Else fetches the relationship object.
	 *
	 * @param  groupID   group id to fetch from
	 * @param  memberID  member id to fetch from
	 *
	 * @return  relationship object (if created / existed)
	 */
	public void removeMembership(String groupID, String memberID) {
		String id = getMembershipID(groupID, memberID);
		if (id != null) {
			relationshipTable.remove(id);
		}
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Utility membership functions, for lookup and filtering
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * Get and list all the related relationship objects, given the memberID, or groupID
	 * 
	 * @param groupID to lookup for
	 * @param memberID to lookup for
	 * 
	 * @return list of relationship objects, matching either the groupID AND/OR memberID (depending on whats given)
	 *         returns a blank lis if no match was found
	 */
	private List<DataObject> listRelationship(String groupID, String memberID) {
		// Optimized query with both groupID and memberID
		if (groupID != null && memberID != null) {
			return Arrays.asList(relationshipTable.query("_groupID = ? AND _memberID = ?",
				new Object[] { groupID, memberID }));
		}
		// groupID based query
		if (groupID != null) {
			return Arrays.asList(relationshipTable.query("_groupID = ?", new Object[] { groupID }));
		}
		// memberID based query
		if (memberID != null) {
			return Arrays.asList(relationshipTable.query("_memberID = ?", new Object[] { memberID }));
		}
		// Above clauses failed, return blank list
		return new ArrayList<>();
	}
	
	/**
	 * Filter membership listing by the given query (where applicable)
	 * 
	 * @param relationshipList to apply filtering on
	 * @param relationshipQuery to apply if given
	 * @param groupQuery to apply if given
	 * @param memberQuery to apply if given
	 * 
	 * @return filtered membership list
	 */
	private List<DataObject> filterRelationship(List<DataObject> relationshipList,
		Query relationshipQuery, Query groupQuery, Query memberQuery) {
		
		// Filtered list to return
		//---------------------------------------------------------------------------------------------
		List<DataObject> filteredList = relationshipList;
		
		// Optimized blank list handling
		//---------------------------------------------------------------------------------------------
		if (filteredList == null || filteredList.size() <= 0) {
			return filteredList;
		}
		
		// Filter by membership first (if given)
		//---------------------------------------------------------------------------------------------
		if (relationshipQuery != null) {
			// filter by relationship query
			filteredList = relationshipQuery.search(filteredList);
			
			// Quick blank list return
			if (filteredList.size() <= 0) {
				return filteredList;
			}
		}
		
		// Filter by group (if given)
		//---------------------------------------------------------------------------------------------
		if (groupQuery != null) {
			// Group mapping
			GenericConvertHashMap<String, Integer> groupStateMap = new GenericConvertHashMap<>();
			
			// Get the group mapping
			for (DataObject mObj : relationshipList) {
				groupStateMap.put(mObj.getString("_groupID"), 0);
			}
			
			// Evaluate each group, and validate using the query
			Set<String> groupKeys = groupStateMap.keySet();
			for (String groupID : groupKeys) {
				// Get the group obj
				DataObject groupObj = groupTable.get(groupID);
				
				// And test it
				if (groupQuery.test(groupObj)) {
					groupStateMap.put(groupID, 1);
				} else {
					groupStateMap.put(groupID, -1);
				}
			}
			
			// Filter the list accordingly
			filteredList = filteredList.stream().filter(mObj -> {
				return groupStateMap.getInt(mObj.getString("_groupID")) == 1;
			}).collect(Collectors.toList());
			
			// Quick blank list return
			if (filteredList.size() <= 0) {
				return filteredList;
			}
		}
		
		// Filter by members (if given)
		//---------------------------------------------------------------------------------------------
		if (memberQuery != null) {
			// Group mapping
			GenericConvertHashMap<String, Integer> memberStateMap = new GenericConvertHashMap<>();
			
			// Get the group mapping
			for (DataObject mObj : relationshipList) {
				memberStateMap.put(mObj.getString("_memberID"), 0);
			}
			
			// Evaluate each group, and validate using the query
			Set<String> memberKeys = memberStateMap.keySet();
			for (String memberID : memberKeys) {
				// Get the group obj
				DataObject memberObj = memberTable.get(memberID);
				
				// And test it
				if (memberQuery.test(memberObj)) {
					memberStateMap.put(memberID, 1);
				} else {
					memberStateMap.put(memberID, -1);
				}
			}
			
			// Filter the list accordingly
			filteredList = filteredList.stream().filter(mObj -> {
				return memberStateMap.getInt(mObj.getString("_memberID")) == 1;
			}).collect(Collectors.toList());
		}
		
		// Return filtered result
		//---------------------------------------------------------------------------------------------
		return filteredList;
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Simple query (@TODO)
	//
	///////////////////////////////////////////////////////////////////////////
	
	///////////////////////////////////////////////////////////////////////////
	//
	// Complex query >_<
	//
	///////////////////////////////////////////////////////////////////////////
	
	/**
	 * List various membership objects, given any combinations of the following complex query options
	 * 
	 * @param groupID                 Optimized groupID based lookup
	 * @param groupQuery              Group query to perform (ignored if groupID is given)
	 * @param groupQueryArgs          Group query args
	 * 
	 * @param memberID                Optimized memberID based lookup
	 * @param memberQuery             Member query to perform (ignored if memberID is given)
	 * @param memberQueryArgs         Member query args
	 * 
	 * @param relationshipQuery       Membership relationship query
	 * @param relationshipQueryArgs   Membership relationship query args
	 * 
	 * Due to its rather complex query nature, it will follow the following query routes to build the query results.
	 * In general it tries to perform the reqspective fetch and join with the assumed "least" impact on the query side for the smallest results.
	 * 
	 * - memberID / groupID is given, fetch its relationship and filter it
	 * - groupQuery is given, fetch the groups and for each group its relationship and filter it
	 * - relationshipQuery is given, fetch the relationship based on query, and filter members listing
	 * - membersQuery is given, fetch relevant members, and for each fetch its relationship.
	 * 
	 * As such it does the complex query
	 * 
	 * @return list of relationship DataObject's
	 */
	public List<DataObject> listMembership_complexMultiQuery(
	// Optimal group ID lookup
		String groupID,
		// Group queyr
		String groupQuery, Object[] groupQueryArgs,
		// Optimal member ID lookup
		String memberID,
		// Member query
		String memberQuery, Object[] memberQueryArgs,
		// Membership query
		String relationshipQuery, Object[] relationshipQueryArgs) {
		
		//------------------------------------------------------------------------------------------
		// Lets prepare all the applicable query objects
		//------------------------------------------------------------------------------------------
		
		// Query objects to use (if applicable)
		Query groupQueryObj = null;
		Query memberQueryObj = null;
		Query relationshipQueryObj = null;
		
		// Build the various query objects
		if (groupID == null && groupQuery != null) {
			groupQueryObj = Query.build(groupQuery, groupQueryArgs);
		}
		if (memberID == null && memberQuery != null) {
			memberQueryObj = Query.build(memberQuery, memberQueryArgs);
		}
		if (relationshipQuery != null) {
			relationshipQueryObj = Query.build(memberQuery, memberQueryArgs);
		}
		
		// The relationship listing (to return?)
		List<DataObject> resultList = new ArrayList<>();
		
		// memberID / groupID based optimized lookup
		//------------------------------------------------------------------------------------------
		if (groupID != null || memberID != null) {
			// Get the relationship list
			resultList = listRelationship(groupID, memberID);
			
			// Does additional filtering ( where applicable ) and return
			return filterRelationship(resultList, relationshipQueryObj, groupQueryObj, memberQueryObj);
		}
		
		//
		// It is safe to assume : groupID == null && memberID == null
		// From this point onwards
		//
		
		// Group based query
		//------------------------------------------------------------------------------------------
		if (groupQueryObj != null) {
			// Get the list of groups
			String[] groupIDStrings = groupTable.query_id(groupQuery, groupQueryArgs);
			
			// For each group, get and add its memberships
			for (String gid : groupIDStrings) {
				// @TODO - consdier getting the group relationship listing WITH the relations query filter
				resultList.addAll(listRelationship(gid, null));
			}
			
			// Filter and return the result list
			// Group query is skipped (as its already done above)
			return filterRelationship(resultList, relationshipQueryObj, null, memberQueryObj);
		}
		
		//
		// It is safe to assume : groupQuery == null
		// From this point onwards
		//
		
		// relationship based query
		//------------------------------------------------------------------------------------------
		if (relationshipQueryObj != null) {
			// Get the list of relevant relationship objects
			resultList = relationshipTable.queryList(relationshipQuery, relationshipQueryArgs);
			
			// Filter by member query (the only one left, if relevant)
			return filterRelationship(resultList, null, null, memberQueryObj);
		}
		
		//
		// It is safe to assume : relationshipQuery == null
		// From this point onwards
		//
		// AKA meaning it should only be the memberQuery left
		//
		
		// member based query
		//------------------------------------------------------------------------------------------
		if (memberQueryObj != null) {
			// Get the list of members
			String[] memberIDStrings = memberTable.query_id(memberQuery, memberQueryArgs);
			
			// For each group, get and add its memberships
			for (String mid : memberIDStrings) {
				resultList.addAll(listRelationship(null, mid));
			}
			
			return resultList;
		}
		
		// blank query - what were you doing
		// ------------------------------------------------------------------------------------------
		throw new IllegalArgumentException(
			"This query requires at least one non null argument to retrieve the dataset");
	}
	
	///////////////////////////////////////////////////////////////////////////
	//
	//  @TODO : A complex query builder (to simplify above)
	//
	///////////////////////////////////////////////////////////////////////////
	
}
