package picoded.module;

import java.util.*;

import picoded.dstack.*;
import picoded.core.conv.*;
import picoded.core.struct.*;
import picoded.core.common.SystemSetupInterface;
import picoded.core.struct.template.AbstractSystemSetupInterfaceCollection;

/**
 * The core module structure in which, every module class has to comply with.
 * This helps provide consistent setup across implementations.
 **/
public abstract class ModuleStructure implements AbstractSystemSetupInterfaceCollection {
	
	//----------------------------------------------------------------
	//
	//  Constructor setup
	//
	//----------------------------------------------------------------
	
	/**
	 * Common stack being used
	 **/
	protected CommonStack stack = null;
	
	/**
	 * The name prefix to use for the module
	 **/
	protected String name = null;
	
	/**
	 * The internal structure list, sued by setup/destroy/maintenance
	 **/
	protected List<CommonStructure> internalStructureList = null;
	
	/**
	 * Blank constructor, used for more custom extensions
	 **/
	public ModuleStructure() {
		// Intentionally left blank
	}
	
	/**
	 * Setup a module structure given a stack, and its name
	 *
	 * @param  CommonStack / DStack system to use
	 * @param  Name used to setup the prefix of the complex structure
	 **/
	public ModuleStructure(CommonStack inStack, String inName) {
		stack = inStack;
		name = inName;
		internalStructureList = internalStructureList();
	}
	
	//----------------------------------------------------------------
	//
	//  Internal CommonStructure management
	//
	//----------------------------------------------------------------
	
	/**
	 * [TO OVERWRITE] : Internal DataStrucutre list,
	 * to cache and pass forward to "SystemSetupInterface"
	 **/
	protected abstract List<CommonStructure> setupInternalStructureList();
	
	/**
	 * Memoizer for setupInternalStructureList
	 * @return
	 */
	protected List<CommonStructure> internalStructureList() {
		if (internalStructureList != null) {
			return internalStructureList;
		}
		internalStructureList = setupInternalStructureList();
		return internalStructureList;
	}
	
	//----------------------------------------------------------------
	//
	//  SystemSetupInterface implementation
	//
	//----------------------------------------------------------------
	
	/**
	 * SystemSetupInterface collection used by subsequent
	 * subcalls via AbstractSystemSetupInterfaceCollection
	 **/
	public Collection<SystemSetupInterface> systemSetupInterfaceCollection() {
		return (Collection<SystemSetupInterface>) (Object) (internalStructureList());
	}
	
}
