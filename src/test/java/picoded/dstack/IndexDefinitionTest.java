package picoded.dstack;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Unit tests for IndexDefinition and its Builder.
 */
public class IndexDefinitionTest {

	@Test
	public void testBuilderAndProperties() {
		IndexDefinition idx = IndexDefinition.create("idx_user_email")
			.ascending("userId")
			.descending("created")
			.unique(true)
			.sparse(true)
			.background(true)
			.expireAfterSeconds(3600L)
			.build();

		assertEquals("idx_user_email", idx.getName());
		assertTrue(idx.isUnique());
		assertTrue(idx.isSparse());
		assertTrue(idx.isBackground());
		assertEquals(Long.valueOf(3600L), idx.getExpireAfterSeconds());

		Map<String, String> keys = idx.getKeys();
		assertEquals(2, keys.size());
		assertEquals("1", keys.get("userId"));
		assertEquals("-1", keys.get("created"));
	}

	@Test
	public void testCustomKeyType() {
		IndexDefinition idx = IndexDefinition.create("idx_geo")
			.key("location", "2dsphere")
			.build();

		assertEquals("2dsphere", idx.getKeys().get("location"));
		assertFalse(idx.isUnique());
		assertFalse(idx.isSparse());
		assertTrue(idx.isBackground());
		assertNull(idx.getExpireAfterSeconds());
	}

	@Test
	public void testMatchesKeysExactOrderAndDirection() {
		IndexDefinition idx1 = IndexDefinition.create("idx_1")
			.ascending("a")
			.descending("b")
			.build();

		IndexDefinition idx2 = IndexDefinition.create("idx_2")
			.ascending("a")
			.descending("b")
			.build();

		// Same keys and same order
		assertTrue(idx1.matchesKeys(idx2));
		assertTrue(idx2.matchesKeys(idx1));

		// Reversed order should not match
		IndexDefinition idxReversed = IndexDefinition.create("idx_rev")
			.descending("b")
			.ascending("a")
			.build();
		assertFalse(idx1.matchesKeys(idxReversed));

		// Different direction should not match
		IndexDefinition idxDiffDir = IndexDefinition.create("idx_diff_dir")
			.ascending("a")
			.ascending("b")
			.build();
		assertFalse(idx1.matchesKeys(idxDiffDir));

		// Subset / superset should not match
		IndexDefinition idxSubset = IndexDefinition.create("idx_sub")
			.ascending("a")
			.build();
		assertFalse(idx1.matchesKeys(idxSubset));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNullKeysConstructorThrows() {
		new IndexDefinition("idx_fail", null, false, false, true, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testEmptyKeysConstructorThrows() {
		new IndexDefinition("idx_fail", new LinkedHashMap<>(), false, false, true, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testBlankFieldNameThrowsInBuilder() {
		IndexDefinition.create("idx_fail").ascending("   ");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNullKeyTypeThrowsInBuilder() {
		IndexDefinition.create("idx_fail").key("field", null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testBuildWithNoKeysThrows() {
		IndexDefinition.create("idx_fail").build();
	}
}
