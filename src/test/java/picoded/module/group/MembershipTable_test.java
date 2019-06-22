package picoded.module.group;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import picoded.core.conv.ConvertJSON;
import picoded.core.conv.GUID;
import picoded.core.struct.GenericConvertHashMap;
import picoded.dstack.DataObject;
import picoded.dstack.DataObjectMap;
import picoded.dstack.struct.simple.StructSimpleStack;
import picoded.dstack.core.CoreStack;

public class MembershipTable_test {
	
	// Test object for reuse
	public CoreStack testStack = null;
	
	public String tablePrefix = "";
	
	// To override for implementation
	// -----------------------------------------------------
	
	/// Note that this implementation constructor
	/// is to be overriden for the various backend
	/// specific test cases
	public CoreStack implementationConstructor() {
		return new StructSimpleStack(new GenericConvertHashMap<String, Object>());
	}
	
	// Setup and sanity test
	// -----------------------------------------------------
	
	DataObjectMap userTable;
	DataObjectMap groupTable;
	DataObjectMap relationshipTable;
	MembershipTable membershipTable;
	
	@Before
	public void systemSetup() {
		testStack = implementationConstructor();
		
		userTable = testStack.dataObjectMap("userTable");
		userTable.systemSetup();
		
		groupTable = testStack.dataObjectMap("groupTable");
		groupTable.systemSetup();
		
		relationshipTable = testStack.dataObjectMap("relationshipTable");
		relationshipTable.systemSetup();
		
		membershipTable = new MembershipTable(groupTable, userTable, relationshipTable);
		membershipTable.systemSetup();
	}
	
	@After
	public void systemDestroy() {
		if (testStack != null) {
			testStack.systemDestroy();
			testStack = null;
		}
		
		if (userTable != null) {
			userTable.systemDestroy();
			userTable = null;
		}
		
		if (groupTable != null) {
			groupTable.systemDestroy();
			groupTable = null;
		}
		
		if (relationshipTable != null) {
			relationshipTable.systemDestroy();
			relationshipTable = null;
		}
		
		if (membershipTable != null) {
			membershipTable.systemDestroy();
			membershipTable = null;
		}
	}
	
	@Test
	public void successfullyAddUserRelationToGroup() {
		
		////////////////////////////////////////////////////////////
		//
		// TEST SETUP
		//
		////////////////////////////////////////////////////////////
		
		// Adding of users
		DataObject user = userTable.newEntry();
		user.put("name", GUID.base58());
		user.put("email", GUID.base58() + "@inboxkitten.com");
		user.saveAll();
		assertNotNull(user);
		assertNotNull(userTable.get(user._oid()));
		
		// Adding of groups
		DataObject group = groupTable.newEntry();
		group.put("name", "GROUP 1");
		group.saveAll();
		assertNotNull(group);
		assertNotNull(groupTable.get(group._oid()));
		
		////////////////////////////////////////////////////////////
		//
		// TEST EXECUTION
		//
		////////////////////////////////////////////////////////////
		
		DataObject relationship = membershipTable.addMembership(group._oid(), user._oid());
		
		assertNotNull(relationship);
		assertEquals(group._oid(), relationship.getString("_groupid"));
		assertEquals(user._oid(), relationship.getString("_memberid"));
		
	}
	
	@Test
	public void successfullyRemoveUserRelation() {
		
		////////////////////////////////////////////////////////////
		//
		// TEST SETUP
		//
		////////////////////////////////////////////////////////////
		
		// Adding of users
		DataObject user = userTable.newEntry();
		user.put("name", GUID.base58());
		user.put("email", GUID.base58() + "@inboxkitten.com");
		user.saveAll();
		assertNotNull(user);
		assertNotNull(userTable.get(user._oid()));
		
		// Adding of groups
		DataObject group = groupTable.newEntry();
		group.put("name", "GROUP 1");
		group.saveAll();
		assertNotNull(group);
		assertNotNull(groupTable.get(group._oid()));
		
		DataObject relationship = membershipTable.addMembership(group._oid(), user._oid());
		
		assertNotNull(relationship);
		assertEquals(group._oid(), relationship.getString("_groupid"));
		assertEquals(user._oid(), relationship.getString("_memberid"));
		
		////////////////////////////////////////////////////////////
		//
		// TEST EXECUTION
		//
		////////////////////////////////////////////////////////////
		
		membershipTable.removeMembership(group._oid(), user._oid());
		
		// Verification
		DataObject verifyRelation = membershipTable.getMembership(group._oid(), user._oid());
		
		assertNull(verifyRelation);
	}
	
	@Test
	public void successfullyListSingleGroupRelation() {
		
		////////////////////////////////////////////////////////////
		//
		// TEST SETUP
		//
		////////////////////////////////////////////////////////////
		
		int number = 3;
		List<String> users = new ArrayList();
		
		// Adding a single group
		DataObject group = groupTable.newEntry();
		group.put("name", "GUID.base58()");
		group.saveAll();
		assertNotNull(group);
		assertNotNull(groupTable.get(group._oid()));
		
		// All users to be added to that group
		for (int i = 0; i < number; i++) {
			// Adding of users
			DataObject user = userTable.newEntry();
			user.put("name", GUID.base58());
			user.put("email", GUID.base58() + "@inboxkitten.com");
			user.saveAll();
			assertNotNull(user);
			assertNotNull(userTable.get(user._oid()));
			
			DataObject relationship = membershipTable.addMembership(group._oid(), user._oid());
			
			assertNotNull(relationship);
			assertEquals(group._oid(), relationship.getString("_groupid"));
			assertEquals(user._oid(), relationship.getString("_memberid"));
			users.add(user._oid());
		}
		
		////////////////////////////////////////////////////////////
		//
		// TEST EXECUTION
		//
		////////////////////////////////////////////////////////////
		
		List<DataObject> relations = membershipTable.listMembership_complexMultiQuery(group._oid(),
			null, null, null, null, null, null, null);
		
		assertTrue("There should be 3 users tagged to 1 group", relations.size() == 3);
		for (DataObject relation : relations) {
			String memberid = relation.getString("_memberid");
			assertTrue(
				"Relation's userID " + memberid + " is not in " + ConvertJSON.fromObject(users),
				users.contains(memberid));
		}
	}
	
}