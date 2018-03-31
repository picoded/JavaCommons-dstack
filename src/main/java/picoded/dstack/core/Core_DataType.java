package picoded.dstack.core;

import java.util.*;

/**
 * DataType enums represents the various data types,
 * that the struct/JSql/JCache/JStack varients of DataTable can support.
 **/
public enum Core_DataType {
	
	//
	// Null or dynamic types
	//--------------------------------------------------------------------
	
	/**
	 * Null field
	 **/
	NULL(0),
	/**
	 * Unspecified storage type, to use auto detection
	/**
	MIXED(1),

	//
	// UUID based
	//--------------------------------------------------------------------

	/**
	 * A UUID identifier
	 **/
	UUID(11),
	/**
	 * Another DataTable identifier. This is used for linked DataTable's
	 * via a commonly agreeded on UUID
	 **/
	DataTable(12),
	
	//
	// Standard based
	//--------------------------------------------------------------------
	
	/**
	 * Integer type
	 **/
	INTEGER(21),
	/**
	 * Long type
	 **/
	LONG(22),
	/**
	 * Double type
	 **/
	DOUBLE(23),
	/**
	 * Float type
	 **/
	FLOAT(24),
	/**
	 * String type
	 **/
	STRING(25),
	
	//
	// Storage types
	//
	// This can be a form of optimziation, as TEXT / BINARY will
	// normally be stored as String if not optimized as such
	//--------------------------------------------------------------------
	
	/**
	 * JSON String storage, can be any internal value
	 * this is used to define the storage format.
	 **/
	JSON(31),
	/**
	 * Text type, this uses TEXT based indexing
	 * (mainly affects SQL layer)
	 **/
	TEXT(32),
	/**
	 * Binary type, this is used to store raw data
	 *
	 * Note that binary type should have a special optimization
	 * involved where data is not pulled until
	 * explitcitely requested for.
	 **/
	BINARY(33),
	
	//
	// Array based, varients of above
	//--------------------------------------------------------------------
	UUID_ARRAY(511), DataTable_ARRAY(512),
	
	INTEGER_ARRAY(521), LONG_ARRAY(522), DOUBLE_ARRAY(523), FLOAT_ARRAY(524), STRING_ARRAY(525),
	
	JSON_ARRAY(531), TEXT_ARRAY(532), BINARY_ARRAY(533);
	
	//////////////////////////////////////////////////////////////////////
	//
	// The following is cookie cutter code,
	//
	// This can be replaced when there is a way to default implement
	// static function and variables, etc, etc.
	//
	// Or the unthinkable, java allow typed macros / type annotations
	// (the closest the language now has to macros)
	//
	// As of now just copy this whole chunk downards, search and
	// replace the class name to its respective enum class to implment
	//
	// Same thing for value type if you want (not recommended)
	//
	//////////////////////////////////////////////////////////////////////
	
	//
	// Constructor setup
	//--------------------------------------------------------------------
	private final int ID;
	
	private Core_DataType(final int inID) {
		ID = inID;
	}
	
	/**
	 * Return the numeric value representing the enum
	 **/
	public int getValue() {
		return ID;
	}
	
	/**
	 * Get name and toString alias to name() varient
	 **/
	public String getName() {
		return super.name();
	}
	
	public String toString() {
		return super.name();
	}
	
	//
	// Public EnumSet
	//--------------------------------------------------------------------
	public static final EnumSet<Core_DataType> typeSet = EnumSet.allOf(Core_DataType.class);
	
	//
	// Type mapping
	//--------------------------------------------------------------------
	
	/**
	 * The type mapping cache
	 **/
	private static Map<String, Core_DataType> nameToTypeMap = null;
	private static Map<Integer, Core_DataType> idToTypeMap = null;
	
	/**
	 * Setting up the type mapping
	 *
	 * Note that the redundent temp variable, is to ensure the final map is only set
	 * in an "atomic" fashion. In event of multiple threads triggerint the initializeTypeMaps
	 * setup process.
	 **/
	protected static void initializeTypeMaps() {
		if (nameToTypeMap == null || idToTypeMap == null) {
			Map<String, Core_DataType> nameToTypeMap_wip = new HashMap<String, Core_DataType>();
			Map<Integer, Core_DataType> idToTypeMap_wip = new HashMap<Integer, Core_DataType>();
			
			for (Core_DataType type : Core_DataType.values()) {
				nameToTypeMap_wip.put(type.name(), type);
				idToTypeMap_wip.put(type.getValue(), type);
			}
			
			nameToTypeMap = nameToTypeMap_wip;
			idToTypeMap_wip = idToTypeMap_wip;
		}
	}
	
	/**
	 * Get from the respective ID values
	 **/
	public static Core_DataType fromID(int id) {
		initializeTypeMaps();
		return idToTypeMap.get(id);
	}
	
	/**
	 * Get from the respective string name values
	 **/
	public static Core_DataType fromName(String name) {
		initializeTypeMaps();
		name = name.toUpperCase();
		return nameToTypeMap.get(name);
	}
	
	/**
	 * Dynamically switches between name, id, or Core_DataType. Null returns null
	 **/
	public static Core_DataType fromTypeObject(Object type) {
		if (type == null) {
			return null;
		}
		
		Core_DataType mType = null;
		if (type instanceof Core_DataType) {
			mType = (Core_DataType) type;
		} else if (type instanceof Number) {
			mType = Core_DataType.fromID(((Number) type).intValue());
		} else {
			mType = Core_DataType.fromName(type.toString());
		}
		
		if (mType == null) {
			throw new RuntimeException("Invalid Core_DataType for: " + type.toString());
		}
		return mType;
	}
	
	//////////////////////////////////////////////////////////////////////
	//
	// End of cookie cutter code,
	//
	//////////////////////////////////////////////////////////////////////
}
