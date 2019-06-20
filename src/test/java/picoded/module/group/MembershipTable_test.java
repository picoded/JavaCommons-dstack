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

	DataObjectMap user;
	DataObjectMap group;
	DataObjectMap relationship;

	MembershipTable membershipTable;

	@Before
	public void systemSetup() {
		user = implementationConstructor();
		user.systemSetup();

		group = implementationConstructor();
		group.systemSetup();

		relationship = implementationConstructor();
		relationship.systemSetup();

		membershipTable = new MembershipTable(group, user, relationship);
		membershipTable.systemSetup();
	}

	@After
	public void systemDestroy() {
		if (testAT != null) {
			testAT.systemDestroy();
			testAT = null;
		}
	}
}