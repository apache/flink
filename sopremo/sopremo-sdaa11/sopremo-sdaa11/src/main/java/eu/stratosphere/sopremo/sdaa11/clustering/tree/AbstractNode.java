package eu.stratosphere.sopremo.sdaa11.clustering.tree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public abstract class AbstractNode implements INode {
	
	private static final long serialVersionUID = 4621528942119500659L;
	
	public static void writeWithType(DataOutput out, INode node) throws IOException {
		if (node instanceof InnerNode) {
			out.writeByte(0);
		} else if (node instanceof Leaf) {
			out.writeByte(1);
		} else {
			throw new IllegalArgumentException("Cannot persist class "+node.getClass());
		}
		node.write(out);
	}
	
	public static INode readWithType(DataInput in, ClusterTree tree) throws IOException {
		INode node;
		byte tag = in.readByte();
		if (tag == 0) {
			node = tree.createInnerNode();
		} else if (tag == 1) {
			node = tree.createLeaf(null, null);
		} else {
			throw new IllegalStateException("Unknown tag: "+tag);
		}
		node.read(in);
		return node;
	}
	
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
