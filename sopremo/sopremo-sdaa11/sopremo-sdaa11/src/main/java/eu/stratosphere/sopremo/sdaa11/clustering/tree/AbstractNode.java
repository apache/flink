package eu.stratosphere.sopremo.sdaa11.clustering.tree;



public abstract class AbstractNode implements INode {
	
	private static final long serialVersionUID = 4621528942119500659L;
	
	protected ClusterTree tree;
	protected InnerNode parent;
	
	protected AbstractNode(ClusterTree tree) {
		this.tree = tree;
	}
	
	public ClusterTree getTree() {
		return tree;
	}
	
	public void setParent(InnerNode parent) {
		this.parent = parent;
	}

}
