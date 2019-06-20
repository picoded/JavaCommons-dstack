package picoded.module.group;

import picoded.picoded.core.CoreStack;
import picoded.picoded.module.group.MembershipTable;

public class MembershipTable_test {

	// Test object for reuse
	public CoreStack testObj = null;

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
		userTable = implementationConstructor();
		userTable.systemSetup();

		groupTable = implementationConstructor();
		groupTable.systemSetup();

		relationshipTable = implementationConstructor();
		relationshipTable.systemSetup();

		membershipTable = new MembershipTable(groupTable, userTable, relationshipTable);
		membershipTable.systemSetup();
	}

	@After
	public void systemDestroy() {
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
	public void successfullyAddUserRElationToGroup(){

		// Adding of users
		DataObject user =  userTable.newEntry();
		user.put("name", "Testing"); 
		user.put("email", "Testing@inboxkitten.com"); 
		user.saveAll();

		// Adding of groups
		DataObject group= groupTable.newEntry();
		group.put("name", "GROUP 1");
		group.saveAll();
		membershipTable.addMembership(group._oid(), user._oid())
	}
}