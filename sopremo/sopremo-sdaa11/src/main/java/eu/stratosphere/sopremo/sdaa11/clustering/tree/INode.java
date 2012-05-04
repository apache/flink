package eu.stratosphere.sopremo.sdaa11.clustering.tree;

import java.io.Serializable;
import java.util.Collection;

import eu.stratosphere.sopremo.sdaa11.JsonSerializable;
import eu.stratosphere.sopremo.sdaa11.clustering.Point;

public interface INode extends Serializable, JsonSerializable {

	/**
	 * Searches itself and children for the leaf that is closest to the given
	 * point.
	 */
	Leaf findLeafNextTo(Point point);

	/**
	 * Adds a new node to this tree (not to its clusters).
	 */
	void add(INode node);

	/**
	 * Returns a collection of all leafs that branch from this node including
	 * this node itself.
	 */
	Collection<Leaf> getLeafs();

	/**
	 * Returns the point which represents the underlying clusters.
	 */
	Point getRepresentantive();

	void setParent(InnerNode node);

	void updateRepresentative();

}
