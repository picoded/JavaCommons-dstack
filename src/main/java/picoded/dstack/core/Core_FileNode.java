package picoded.dstack.core;

import picoded.dstack.FileNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class Core_FileNode extends HashMap<String, Object> implements FileNode {
	
	private List<FileNode> nodes;
	
	public Core_FileNode(String name, boolean isFolder) {
		this.put("name", name);
		this.put("folder", isFolder);
		this.put("type", isFolder ? "directory" : "file");
		nodes = new ArrayList<>();
		this.put("children", nodes);
	}
	
	@Override
	public String name() {
		return this.get("name").toString();
	}
	
	@Override
	public boolean isFolder() {
		return (boolean) this.get("folder");
	}
	
	@Override
	public String type() {
		return this.get("type").toString();
	}
	
	@Override
	public List<FileNode> nodes() {
		return (List<FileNode>) this.get("children");
	}
	
	@Override
	public void add(FileNode fileNode) {
		nodes.add(fileNode);
	}
	
	@Override
	public void removeChildrenNodes() {
		this.remove("children");
	}
}
