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
	
	public Point() { }
	
	public Point(String key, List<String> values) {
		this.key = key;
		this.values = values;
	}
	
	public String getKey() {
		return key;
	}

	public List<String> getValues() {
		return new ArrayList<String>(values);
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}

	public int getRowsum() {
		return rowsum;
	}

	public void addToRowsum(Point point) {
		rowsum += getSquaredDistance(point);
	}

	public void setRowsum(int rowsum) {
		this.rowsum = rowsum;
	}

	public void setRowsum(Collection<Point> points) {
		rowsum = 0;
		for (Point point : points) {
			addToRowsum(point);
		}
	}
	
	public int calculateRowsum(Collection<Point> points) {
		int rowsum = 0;
		for (Point point : points) {
			rowsum += getSquaredDistance(point);
		}
		return rowsum;
	}
	
	public int getSquaredDistance(Point point) {
		int distance = getDistance(point);
		return distance * distance;
	}

	public int getDistance(Point point) {
		return SortedJaccardDistance.distance(getValues(), point.getValues(), FastStringComparator.INSTANCE);
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(key);
		
		out.writeInt(values.size());
		for (String value : values) {
			out.writeUTF(value);
		}
		
		out.writeInt(rowsum);
	}

	public void read(DataInput in) throws IOException {
		key = in.readUTF();
		
		int valuesSize = in.readInt();
		if (values == null) {
			values = new ArrayList<String>(valuesSize);
		} else {
			values.clear();
		}
		for (int i=0; i < valuesSize; i++) {
			values.add(in.readUTF());
		}
		
		rowsum = in.readInt();
	}
	
	@Override
	public Point clone() {
		Point point = new Point();
		point.key = key;
		point.values = new ArrayList<String>(values);
		point.rowsum = rowsum;
		return point;
	}
	
	@Override
	public String toString() {
		return key;
	}

	@Override
	public int compareTo(Point otherPoint) {
		int diff = rowsum - otherPoint.getRowsum();
		if (diff != 0) return diff;
		return FastStringComparator.INSTANCE.compare(key, otherPoint.key);
	}

	public IJsonNode toJsonNode(ObjectNode node) {
		if (node == null)
			node = new ObjectNode();
		
		node.put("id", new TextNode(key));
		ArrayNode valuesNode = new ArrayNode();
		for (String value : values) {
			valuesNode.add(new TextNode(value));
		}
		node.put("values", valuesNode);
		node.put("rowsum", new IntNode(rowsum));
		
		return node;
	}
}
