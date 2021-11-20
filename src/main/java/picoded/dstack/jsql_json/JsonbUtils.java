package picoded.dstack.jsql;

import java.util.logging.*;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import picoded.core.security.NxtCrypt;
import picoded.dstack.DataObjectMap;
import picoded.dstack.DataObject;
import picoded.dstack.core.Core_DataObjectMap;
import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.query.Query;
import picoded.core.struct.GenericConvertHashMap;
import picoded.dstack.connector.jsql.*;
import picoded.core.conv.ListValueConv;
import picoded.core.conv.ConvertJSON;
import picoded.core.struct.GenericConvertMap;
import picoded.core.struct.GenericConvertList;
import picoded.core.struct.MutablePair;
import picoded.core.common.ObjectToken;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Various JSONB specific utilities, used internally to handle data processing
 **/
public class JsonbUtils {
	
	/**
	 * ThreadLocal copy of kryo - implemented as refrenced from
	 * https://hazelcast.com/blog/kryo-serializer/
	 */
	private static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
		@Override
		protected Kryo initialValue() {
			Kryo kryo = new Kryo();
			kryo.register(HashMap.class);
			return kryo;
		}
	};
	
	/**
	 * Serializing the data map into two object pairs consisting of
	 * - a JSON string
	 * - serialized data binary (eg. byte[])
	 *
	 * @param {Map<String,Object>} objMap - map to extract values to store from
	 *
	 * @return Converted pairs of 2 objects
	 */
	public static MutablePair<String, byte[]> serializeDataMap(Map<String, Object> inMap) {
		// The json and bin map, to encode seprately
		Map<String, Object> jsonMap = new HashMap<String, Object>();
		Map<String, Object> binMap = new HashMap<String, Object>();
		
		// Strictly speaking, the implmentation support of other 
		// JSQL (non JSON) backend does not support the use of byte[] data
		// in nested dataset.
		// 
		// While this is not a limitation of the current backend design
		// we make the same presumption, while avoiding the need
		// for recursive scans / conversion
		Set<String> keySet = inMap.keySet();
		
		// Iterate the key list to apply updates
		for (String k : keySet) {
			// Get the value
			Object v = inMap.get(k);
			
			// Skip reserved key, otm is not allowed to be saved
			// (to ensure blank object is saved)
			if (k.equalsIgnoreCase("_otm")) { //reserved
				continue;
			}
			
			// Key length size protection
			if (k.length() > 64) {
				throw new RuntimeException(
					"Attempted to insert a key value larger then 64 for (_oid = " + inMap.get("_oid")
						+ "): " + k);
			}
			
			// Delete support, ignore NULL values
			if (v == ObjectToken.NULL || v == null) {
				// Skip reserved key, oid key is NOT allowed to be removed directly
				if (k.equalsIgnoreCase("oid") || k.equalsIgnoreCase("_oid")) {
					continue;
				}
			} else if (v instanceof byte[]) {
				// Handling of binary data
				binMap.put(k, v);
			} else {
				// In all other cases, treat it as JSON data
				jsonMap.put(k, v);
			}
		}
		
		// Lets do the required conversions
		String json = ConvertJSON.fromMap(jsonMap);
		byte[] bin = null;
		
		// Count the keyset
		if (binMap.keySet().size() > 0) {
			// Lets encode the binMap
			
			// Get the kyro instance
			Kryo kryo = kryoThreadLocal.get();
			
			// Setup the default byte array stream 
			// @CONSIDER: Should we initialize with (16kb buffer?) `new ByteArrayOutputStream(16384)`
			ByteArrayOutputStream BA_OutputStream = new ByteArrayOutputStream();
			DeflaterOutputStream D_OutputStream = new DeflaterOutputStream(BA_OutputStream);
			Output kyroOutput = new Output(D_OutputStream);
			
			// Write the object, into the output stream
			kryo.writeObject(kyroOutput, binMap);
			kyroOutput.close();
			
			// Output into a bin byte[]
			bin = BA_OutputStream.toByteArray();
		}
		
		// Return the full result pair.
		return new MutablePair<String, byte[]>(json, bin);
	}
	
	/**
	 * DeSerializing the data map from the two object pairs consisting of
	 * - a JSON string
	 * - serialized data binary (eg. byte[])
	 *
	 * @param {String} the json data string
	 * @param {byte[]} the binary data
	 *
	 * @return Full Map of both data
	 */
	public static Map<String, Object> deserializeDataMap(String jsonData, byte[] binData) {
		// The json map
		Map<String, Object> jsonMap = ConvertJSON.toMap(jsonData);
		
		// if bin data is null, return the json data as it is
		if (binData == null) {
			return jsonMap;
		}
		
		// Lets process the bin data
		ByteArrayInputStream BA_InputStream = new ByteArrayInputStream(binData);
		InflaterInputStream I_InputStream = new InflaterInputStream(BA_InputStream);
		Input kyroInput = new Input(I_InputStream);
		
		// Get the kyro instance
		Kryo kryo = kryoThreadLocal.get();
		
		// Read the binary data map
		Map<String, Object> binMap = kryo.readObject(kyroInput, HashMap.class);
		
		// Build the return map
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.putAll(jsonMap);
		retMap.putAll(binMap);
		
		// And return
		return retMap;
	}
}