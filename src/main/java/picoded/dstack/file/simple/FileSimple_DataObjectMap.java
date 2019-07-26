//
//  ON-HOLD
//
//  Due to unspecified behaviour on how certain complex data would be handled,
//  such as byte[]. The implementation is "on-hold"
//
//  Possible solutions include adding GSON support
//  https://gist.github.com/orip/3635246
//

// package picoded.dstack.file.simple;

// import picoded.dstack.core.Core_DataObjectMap;

// import java.io.File;
// import java.util.HashMap;
// import java.util.Map;
// import java.util.Set;
// import java.util.concurrent.locks.ReentrantReadWriteLock;

// /**
//  * File system backed version of DataObjectMap
//  * 
//  * This allows dstack to function purely using the filesystem,
//  * which would be convinent for development purpose.
//  */
// public class FileSimple_DataObjectMap extends Core_DataObjectMap {

// 	//--------------------------------------------------------------------------
// 	//
// 	// Constructor vars
// 	//
// 	//--------------------------------------------------------------------------

// 	/// The file directory to opreate from
// 	protected File baseDir = null;

// 	/// The file suffix to use for JSON object records

// 	/**
// 	 * Setup with file directory
// 	 * 
// 	 * @param  inDir folder directory to operate from
// 	 */
// 	public FileSimple_DataObjectMap(File inDir) { 
// 		baseDir = inDir;
// 	}

// 	/**
// 	 * Setup with file directory
// 	 * 
// 	 * @param  inDir folder directory to operate from
// 	 */
// 	public FileSimple_DataObjectMap(String inDir) {
// 		baseDir = new File(inDir);
// 	}

// }