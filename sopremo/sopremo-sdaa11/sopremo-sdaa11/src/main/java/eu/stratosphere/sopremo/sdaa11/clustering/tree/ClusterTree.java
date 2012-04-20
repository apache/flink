package eu.stratosphere.sopremo.sdaa11.clustering.tree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.sopremo.sdaa11.clustering.Point;

public class ClusterTree implements Serializable, Value {
	
	private static final long serialVersionUID = -2054155381249234100L;

	private InnerNode root;
	private int degree;
	
	public ClusterTree() { }
	
	public ClusterTree(int degree) {
		this.degree = degree;
		this.root = createInnerNode();
	}

	public Leaf createLeaf(Point clustroid, String clusterId) {
		return new Leaf(this, clustroid, clusterId);
	}

	public InnerNode createInnerNode() {
		return new InnerNode(this, degree);
	}
	
	public void add(Point clustroid, String clusterId) {
		root.add(createLeaf(clustroid, clusterId));
	}

	/* Hack'n'slay */
	private void printTree(INode node, int indent, StringBuilder sb) {
		for (int i = 0; i < indent; i++) {
			sb.append("  ");
		}
		sb.append(node).append("\n");
		if (node instanceof InnerNode) {
			InnerNode innerNode = (InnerNode) node;
			for (INode subnode : innerNode.getSubnodes()) {
				printTree(subnode, indent + 1, sb);
			}
		}
	}

	public void remove(INode node) {
		if (root.equals(node)) {
			root = createInnerNode();
		}
		root.remove(node);
	}

	public String findIdOfClusterNextTo(Point point) {
		return root.findLeafNextTo(point).getClusterId();
	}
	
	
	public InnerNode merge(INode node1, INode node2) {
		InnerNode mergedNode = createInnerNode();
		addAll(node1, mergedNode);
		addAll(node2, mergedNode);
		return mergedNode;
	}

	private void addAll(INode node, InnerNode mergedNode) {
		if (node instanceof Leaf) {
			mergedNode.add(node);
		} else {
			InnerNode innerNode = (InnerNode) node;
			for (INode subnode : innerNode.getSubnodes()) {
				mergedNode.add(subnode);
			}
		}
	}

	public Collection<Leaf> getLeafs() {
		return root.getLeafs();
	}

	public List<Point> getClustroids() {
		Collection<Leaf> leafs = getLeafs();
		List<Point> clustroids = new ArrayList<Point>(leafs.size());
		for (Leaf leaf : leafs) {
			clustroids.add(leaf.getClustroid());
		}
		return clustroids;
	}

	public List<String> getClusterIds() {
		Collection<Leaf> leafs = getLeafs();
		List<String> ids = new ArrayList<String>(leafs.size());
		for (Leaf leaf : leafs) {
			ids.add(leaf.getClusterId());
		}
		return ids;
	}

	public InnerNode getRootNode() {
		return root;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("GRGPF Tree: \n");
		printTree(root, 0, sb);
		return sb.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(degree);
		root.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		degree = in.readInt();
		root = createInnerNode();
		root.read(in);
	}

}
