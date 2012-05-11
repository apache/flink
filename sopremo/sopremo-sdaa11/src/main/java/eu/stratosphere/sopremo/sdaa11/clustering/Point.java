package eu.stratosphere.sopremo.sdaa11.clustering;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import eu.stratosphere.sopremo.sdaa11.JsonSerializable;
import eu.stratosphere.sopremo.sdaa11.clustering.json.PointNodes;
import eu.stratosphere.sopremo.sdaa11.clustering.util.SortedJaccardDistance;
import eu.stratosphere.sopremo.sdaa11.util.FastStringComparator;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class Point implements Serializable, Cloneable, Comparable<Point>,
		JsonSerializable {

	private static final long serialVersionUID = 8916618314991854207L;

	private String key;
	private List<String> values;
	private int rowsum = 0;

	public Point() {
	}

	public Point(final String key, final String... values) {
		this(key, Arrays.asList(values));
	}

	public Point(final String key, final List<String> values) {
		this.key = key;
		this.values = values;
	}

	public String getKey() {
		return this.key;
	}

	public List<String> getValues() {
		return new ArrayList<String>(this.values);
	}

	@Override
	public int hashCode() {
		return this.key.hashCode();
	}

	public int getRowsum() {
		return this.rowsum;
	}

	public void addToRowsum(final Point point) {
		this.rowsum += this.getSquaredDistance(point);
	}

	public void setRowsum(final int rowsum) {
		this.rowsum = rowsum;
	}

	public void setRowsum(final Collection<Point> points) {
		this.rowsum = 0;
		for (final Point point : points)
			this.addToRowsum(point);
	}

	public int calculateRowsum(final Collection<Point> points) {
		int rowsum = 0;
		for (final Point point : points)
			rowsum += this.getSquaredDistance(point);
		return rowsum;
	}

	public int getSquaredDistance(final Point point) {
		final int distance = this.getDistance(point);
		return distance * distance;
	}

	public int getDistance(final Point point) {
		return SortedJaccardDistance.distance(this.getValues(),
				point.getValues(), FastStringComparator.INSTANCE);
	}

	@Override
	public Point clone() {
		final Point point = new Point();
		point.key = this.key;
		point.values = new ArrayList<String>(this.values);
		point.rowsum = this.rowsum;
		return point;
	}

	@Override
	public String toString() {
		return this.key;
	}

	@Override
	public int compareTo(final Point otherPoint) {
		final int diff = this.rowsum - otherPoint.getRowsum();
		if (diff != 0)
			return diff;
		return FastStringComparator.INSTANCE.compare(this.key, otherPoint.key);
	}

	@Override
	public IJsonNode write(final IJsonNode node) {
		ObjectNode objectNode;
		if (node == null || !(node instanceof ObjectNode))
			objectNode = new ObjectNode();
		else
			objectNode = (ObjectNode) node;

		final ArrayNode valuesNode = new ArrayNode();
		for (final String value : this.values)
			valuesNode.add(new TextNode(value));
		PointNodes.write(objectNode, new TextNode(this.key), valuesNode,
				new IntNode(this.rowsum));

		return objectNode;
	}

	@Override
	public void read(final IJsonNode node) {
		if (node == null || !(node instanceof ObjectNode))
			throw new IllegalArgumentException("Illegal point node: " + node);
		final ObjectNode objectNode = (ObjectNode) node;

		this.key = PointNodes.getId(objectNode).getJavaValue();
		this.values = new ArrayList<String>();
		for (final IJsonNode valuesNode : PointNodes.getValues(objectNode))
			this.values.add(((TextNode) valuesNode).getJavaValue());
		this.rowsum = PointNodes.getRowsum(objectNode).getJavaValue();
	}
}
