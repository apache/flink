package eu.stratosphere.sopremo.sdaa11.clustering.tree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import eu.stratosphere.sopremo.sdaa11.clustering.Point;

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

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(clusterId);
		clustroid.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		clusterId = in.readUTF();
		clustroid = new Point();
		clustroid.read(in);
	}

}
