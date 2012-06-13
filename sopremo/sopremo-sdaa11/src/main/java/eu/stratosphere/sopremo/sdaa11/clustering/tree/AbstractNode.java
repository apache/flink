package eu.stratosphere.sopremo.sdaa11.clustering.tree;

public abstract class AbstractNode implements INode {

	private static final long serialVersionUID = 4621528942119500659L;

	public static final String JSON_KEY_CHILDREN = "children";
	public static final String JSON_KEY_REPRESENTATIVE = "representative";
	public static final String JSON_KEY_CHILD = "child";
	public static final String JSON_KEY_ROWSUM = "rowsum";
	public static final String JSON_KEY_HAS_LEAF = "containsLeaf";
	public static final String JSON_KEY_ID = "id";
	public static final String JSON_KEY_CLUSTROID = "clustroid";

	protected ClusterTree tree;
	protected InnerNode parent;

	protected AbstractNode(final ClusterTree tree) {
		this.tree = tree;
	}

	public ClusterTree getTree() {
		return this.tree;
	}

	@Override
	public void setParent(final InnerNode parent) {
		this.parent = parent;
	}

}
