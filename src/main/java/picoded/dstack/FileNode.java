package picoded.dstack;

import picoded.core.struct.GenericConvertMap;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface FileNode {
	
	public String name();
	
	public boolean isFolder();
	
	public String type();
	
	public List<FileNode> nodes();
	
	public void add(FileNode fileNode);
}
