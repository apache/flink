package eu.stratosphere.sopremo.sdaa11.clustering.tree;

import java.util.Arrays;
import java.util.Collection;

import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.util.JsonUtil2;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class Leaf extends AbstractNode {

	private static final long serialVersionUID = -6632448760861953546L;

	private Point clustroid;
	private String clusterId;

	public Leaf(final ClusterTree tree, final Point clustroid,
			final String clusterId) {
		super(tree);
		this.clustroid = clustroid;
		this.clusterId = clusterId;
		this.updateRepresentative();
	}

	@Override
	public void add(final INode node) {
		final AbstractNode newNode = this.tree.createInnerNode();
		newNode.add(node);
		this.parent.replace(this, newNode);
		newNode.add(this);
	}

	public Point getClustroid() {
		return this.clustroid;
	}

	public String getClusterId() {
		return this.clusterId;
	}

	@Override
	public void updateRepresentative() {
		if (this.parent != null)
			this.parent.updateRepresentative();
	}

	@Override
	public Point getRepresentantive() {
		return this.clustroid;
	}

	@Override
	public Collection<Leaf> getLeafs() {
		return Arrays.asList(this);
	}

	@Override
	public String toString() {
		return "Leaf[" + this.clustroid + "]";
	}

	@Override
	public Leaf findLeafNextTo(final Point point) {
		return this;
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
		objectNode.put(JSON_KEY_ID, new TextNode(this.clusterId));
		objectNode.put(JSON_KEY_CLUSTROID, this.clustroid.write(null));
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
		final ObjectNode objectNode = (ObjectNode) node;
		this.clusterId = ((TextNode) objectNode.get(JSON_KEY_ID)).getTextValue();
		this.clustroid = new Point();
		this.clustroid.read(objectNode.get(JSON_KEY_CLUSTROID));
	}

}
