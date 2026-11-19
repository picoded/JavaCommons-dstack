package picoded.dstack;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Normalized, backend-agnostic index definition representing collection/table index specifications.
 * Defines index name, indexed fields (field name to direction/type, preserving order), and options.
 */
public class IndexDefinition {

	public static final String ASCENDING = "1";
	public static final String DESCENDING = "-1";

	private final String name;
	private final LinkedHashMap<String, String> keys;
	private final boolean unique;
	private final boolean sparse;
	private final boolean background;
	private final Long expireAfterSeconds;

	/**
	 * Construct a new IndexDefinition.
	 *
	 * @param name index identifier name (optional, backend can auto-generate)
	 * @param keys map of field names to direction or type ("1", "-1", "text", etc.), preserving order
	 * @param unique whether unique constraint is enforced
	 * @param sparse whether index is sparse
	 * @param background whether index is built in background
	 * @param expireAfterSeconds TTL expiration in seconds, or null if disabled
	 */
	public IndexDefinition(String name, Map<String, String> keys, boolean unique, boolean sparse, boolean background, Long expireAfterSeconds) {
		if (keys == null || keys.isEmpty()) {
			throw new IllegalArgumentException("Index keys cannot be null or empty");
		}
		// Validate that no key field name or direction is null or empty
		for (Map.Entry<String, String> entry : keys.entrySet()) {
			if (entry.getKey() == null || entry.getKey().trim().isEmpty()) {
				throw new IllegalArgumentException("Index field name cannot be null or empty");
			}
			if (entry.getValue() == null || entry.getValue().trim().isEmpty()) {
				throw new IllegalArgumentException("Index direction/type for field '" + entry.getKey() + "' cannot be null or empty");
			}
		}
		this.name = name;
		this.keys = new LinkedHashMap<>(keys);
		this.unique = unique;
		this.sparse = sparse;
		this.background = background;
		this.expireAfterSeconds = expireAfterSeconds;
	}

	public String getName() {
		return name;
	}

	public Map<String, String> getKeys() {
		return Collections.unmodifiableMap(keys);
	}

	public boolean isUnique() {
		return unique;
	}

	public boolean isSparse() {
		return sparse;
	}

	public boolean isBackground() {
		return background;
	}

	public Long getExpireAfterSeconds() {
		return expireAfterSeconds;
	}

	/**
	 * Checks whether this index definition matches the keys and key order of another index definition.
	 *
	 * @param other target index definition to compare against
	 * @return true if all field names, directions/types, and insertion orders match exactly
	 */
	public boolean matchesKeys(IndexDefinition other) {
		if (other == null || other.keys == null || this.keys.size() != other.keys.size()) {
			return false;
		}
		Iterator<Map.Entry<String, String>> it1 = this.keys.entrySet().iterator();
		Iterator<Map.Entry<String, String>> it2 = other.keys.entrySet().iterator();
		while (it1.hasNext() && it2.hasNext()) {
			Map.Entry<String, String> e1 = it1.next();
			Map.Entry<String, String> e2 = it2.next();
			if (!Objects.equals(e1.getKey(), e2.getKey()) || !Objects.equals(e1.getValue(), e2.getValue())) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Convenience fluent builder factory.
	 *
	 * @param name index identifier name
	 * @return new Builder instance
	 */
	public static Builder create(String name) {
		return new Builder(name);
	}

	/**
	 * Fluent builder for IndexDefinition instances.
	 */
	public static class Builder {
		private final String name;
		private final LinkedHashMap<String, String> keys = new LinkedHashMap<>();
		private boolean unique = false;
		private boolean sparse = false;
		private boolean background = true;
		private Long expireAfterSeconds = null;

		public Builder(String name) {
			this.name = name;
		}

		public Builder ascending(String field) {
			if (field == null || field.trim().isEmpty()) {
				throw new IllegalArgumentException("Index field name cannot be null or empty");
			}
			keys.put(field.trim(), ASCENDING);
			return this;
		}

		public Builder descending(String field) {
			if (field == null || field.trim().isEmpty()) {
				throw new IllegalArgumentException("Index field name cannot be null or empty");
			}
			keys.put(field.trim(), DESCENDING);
			return this;
		}

		public Builder key(String field, String typeOrDirection) {
			if (field == null || field.trim().isEmpty()) {
				throw new IllegalArgumentException("Index field name cannot be null or empty");
			}
			if (typeOrDirection == null || typeOrDirection.trim().isEmpty()) {
				throw new IllegalArgumentException("Index direction or type cannot be null or empty");
			}
			keys.put(field.trim(), typeOrDirection.trim());
			return this;
		}

		public Builder unique(boolean unique) {
			this.unique = unique;
			return this;
		}

		public Builder sparse(boolean sparse) {
			this.sparse = sparse;
			return this;
		}

		public Builder background(boolean background) {
			this.background = background;
			return this;
		}

		public Builder expireAfterSeconds(Long seconds) {
			this.expireAfterSeconds = seconds;
			return this;
		}

		public IndexDefinition build() {
			return new IndexDefinition(name, keys, unique, sparse, background, expireAfterSeconds);
		}
	}
}
