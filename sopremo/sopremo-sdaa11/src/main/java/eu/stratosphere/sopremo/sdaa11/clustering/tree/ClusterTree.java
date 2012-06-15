package eu.stratosphere.sopremo.sdaa11.clustering.tree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import eu.stratosphere.sopremo.sdaa11.JsonSerializable;
import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.util.JsonUtil2;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;

public class ClusterTree implements Serializable, JsonSerializable {

	private static final String JSON_KEY_ROOT = "root";
	private static final String JSON_KEY_DEGREE = "degree";

	private static final long serialVersionUID = -2054155381249234100L;

	private InnerNode root;
	private int degree;

	public ClusterTree() {
	}

	public ClusterTree(final int degree) {
		this.degree = degree;
		this.root = this.createInnerNode();
	}

	public Leaf createLeaf(final Point clustroid, final String clusterId) {
		return new Leaf(this, clustroid, clusterId);
	}

	public InnerNode createInnerNode() {
		return new InnerNode(this, this.degree);
	}

	public void add(final Point clustroid, final String clusterId) {
		this.root.add(this.createLeaf(clustroid, clusterId));
	}

	/* Hack'n'slay */
	private void printTree(final INode node, final int indent,
			final StringBuilder sb) {
		for (int i = 0; i < indent; i++)
			sb.append("  ");
		sb.append(node).append("\n");
		if (node instanceof InnerNode) {
			final InnerNode innerNode = (InnerNode) node;
			for (final INode subnode : innerNode.getSubnodes())
				this.printTree(subnode, indent + 1, sb);
		}
	}

	public void remove(final INode node) {
		if (this.root.equals(node))
			this.root = this.createInnerNode();
		this.root.remove(node);
	}

	public String findIdOfClusterNextTo(final Point point) {
		return this.root.findLeafNextTo(point).getClusterId();
	}

	public AbstractNode merge(final INode node1, final INode node2) {
		final AbstractNode mergedNode = this.createInnerNode();
		this.addAll(node1, mergedNode);
		this.addAll(node2, mergedNode);
		return mergedNode;
	}

	private void addAll(final INode node, final AbstractNode mergedNode) {
		if (node instanceof Leaf)
			mergedNode.add(node);
		else {
			final InnerNode innerNode = (InnerNode) node;
			for (final INode subnode : innerNode.getSubnodes())
				mergedNode.add(subnode);
		}
	}

	public Collection<Leaf> getLeafs() {
		return this.root.getLeafs();
	}

	public List<Point> getClustroids() {
		final Collection<Leaf> leafs = this.getLeafs();
		final List<Point> clustroids = new ArrayList<Point>(leafs.size());
		for (final Leaf leaf : leafs)
			clustroids.add(leaf.getClustroid());
		return clustroids;
	}

	public List<String> getClusterIds() {
		final Collection<Leaf> leafs = this.getLeafs();
		final List<String> ids = new ArrayList<String>(leafs.size());
		for (final Leaf leaf : leafs)
			ids.add(leaf.getClusterId());
		return ids;
	}

	public AbstractNode getRootNode() {
		return this.root;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("GRGPF Tree: \n");
		this.printTree(this.root, 0, sb);
		return sb.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.sdaa11.JsonSerializable#write(eu.stratosphere
	 * .sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode write(final IJsonNode node) {
		final ObjectNode objectNode = JsonUtil2.reuseObjectNode(node);
		objectNode.put(JSON_KEY_DEGREE, new IntNode(this.degree));
		objectNode.put(JSON_KEY_ROOT, this.root.write(null));
		return objectNode;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.sdaa11.JsonSerializable#read(eu.stratosphere.
	 * sopremo.type.IJsonNode)
	 */
	@Override
	public void read(final IJsonNode node) {
		try {
			this.degree = JsonUtil2.getField(node, JSON_KEY_DEGREE,
					IntNode.class).getIntValue();
		} catch (final ClassCastException e) {
			System.out.println("Erronous node: " + node);
			throw e;
		}
		this.root = this.createInnerNode();
		this.root.read(JsonUtil2
				.getField(node, JSON_KEY_ROOT, ObjectNode.class));
	}

}
