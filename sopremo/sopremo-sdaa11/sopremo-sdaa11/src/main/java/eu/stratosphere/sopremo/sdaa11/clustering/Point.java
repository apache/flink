package eu.stratosphere.sopremo.sdaa11.clustering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.sopremo.sdaa11.clustering.util.SortedJaccardDistance;
import eu.stratosphere.sopremo.sdaa11.util.FastStringComparator;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class Point implements Value, Serializable, Cloneable, Comparable<Point> {

	private static final long serialVersionUID = 8916618314991854207L;

	private String key;
	private List<String> values;
	private int rowsum = 0;

	public Point() {
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
	public void write(final DataOutput out) throws IOException {
		out.writeUTF(this.key);

		out.writeInt(this.values.size());
		for (final String value : this.values)
			out.writeUTF(value);

		out.writeInt(this.rowsum);
	}

	@Override
	public void read(final DataInput in) throws IOException {
		this.key = in.readUTF();

		final int valuesSize = in.readInt();
		if (this.values == null)
			this.values = new ArrayList<String>(valuesSize);
		else
			this.values.clear();
		for (int i = 0; i < valuesSize; i++)
			this.values.add(in.readUTF());

		this.rowsum = in.readInt();
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

	public IJsonNode toJsonNode(ObjectNode node) {
		if (node == null)
			node = new ObjectNode();

		node.put("id", new TextNode(this.key));
		final ArrayNode valuesNode = new ArrayNode();
		for (final String value : this.values)
			valuesNode.add(new TextNode(value));
		node.put("values", valuesNode);
		node.put("rowsum", new IntNode(this.rowsum));

		return node;
	}
	
	public void fromJsonNode(IJsonNode node) {
		if (node == null || !(node instanceof ObjectNode)) {
			throw new IllegalArgumentException("Illegal point node: "+node);
		}
		ObjectNode objectNode = (ObjectNode) node;
		key = ((TextNode) objectNode.get("id")).getJavaValue();
		values = new ArrayList<String>();
		for (IJsonNode valuesNode : (ArrayNode) objectNode.get("values")) {
			values.add(((TextNode) valuesNode).getJavaValue());
		}
		rowsum = ((IntNode) objectNode.get("rowsum")).getJavaValue();
	}
}
