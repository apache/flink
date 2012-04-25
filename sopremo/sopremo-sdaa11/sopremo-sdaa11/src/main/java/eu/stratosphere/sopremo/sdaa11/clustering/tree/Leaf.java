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

	public Leaf(ClusterTree tree, Point clustroid, String clusterId) {
		super(tree);
		this.clustroid = clustroid;
		this.clusterId = clusterId;
		updateRepresentative();
	}
	
	@Override
	public void add(INode node) {
		InnerNode newNode = tree.createInnerNode();
		newNode.add(node);
		parent.replace(this, newNode);
		newNode.add(this);
	}
	
	public Point getClustroid() {
		return clustroid;
	}
	
	public String getClusterId() {
		return clusterId;
	}
	
	@Override
	public void updateRepresentative() {
		if (parent != null) {
			parent.updateRepresentative();
		}
	}
	
	@Override
	public Point getRepresentantive() {
		return clustroid;
	}
	
	@Override
	public Collection<Leaf> getLeafs() {
		return Arrays.asList(this);
	}

	@Override
	public String toString() {
		return "Leaf["+clustroid+"]";
	}

	@Override
	public Leaf findLeafNextTo(Point point) {
		return this;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.sdaa11.JsonSerializable#write(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode write(IJsonNode node) {
		ObjectNode objectNode = JsonUtil2.reuseObjectNode(node);
		objectNode.put("id", new TextNode(clusterId));
		objectNode.put("clustroid", clustroid.write(null));
		return objectNode;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.sdaa11.JsonSerializable#read(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public void read(IJsonNode node) {
		ObjectNode objectNode = (ObjectNode) node;
		clusterId = ((TextNode) objectNode.get("id")).getTextValue();
		clustroid = new Point();
		clustroid.read(objectNode.get("clustroid"));
	}

}
