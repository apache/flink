package eu.stratosphere.sopremo.sdaa11.clustering.tree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.util.JsonUtil2;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;


public class InnerNode extends AbstractNode {
	
	private static final long serialVersionUID = -1271061202394454550L;

	private static class Entry implements Serializable {

		private static final long serialVersionUID = 6320924514526694129L;
		
		private Entry(INode subnode) {
			this(subnode, -1);
		}
		private Entry(INode subnode, int rowsum) {
			this.subnode = subnode;
		}
		private INode subnode;
		private int rowsum;
		private Point getPoint() {
			return subnode.getRepresentantive();
		}

	}
	
	private Entry[] entries;
	private Point representative;
	
	public InnerNode(ClusterTree tree, int degree) {
		super(tree);
		this.entries = new Entry[degree];
	}

	private INode findClosestChild(Point point) {
		int minDistance = -1;
		INode subnode = null;
		for (Entry entry : entries) {
			if (entry != null) {
				int tempDistance = point.getDistance(entry.getPoint());
				if (minDistance < 0 || tempDistance < minDistance) {
					minDistance = tempDistance;
					subnode = entry.subnode;
				}
			}
		}
		if (subnode == null) {
			throw new IllegalStateException("Did not find a subnode");
		}
		return subnode;
	}

	@Override
	public void add(INode node) {
		int freeIndex = findFreeIndex();
		if (freeIndex < 0) {
			int substitutionIndex = findSubstitutionIndex(node.getRepresentantive());
			if (substitutionIndex == -1) {
				addToChild(node);
			} else {
				Entry oldEntry = entries[substitutionIndex];
				setEntry(substitutionIndex, node);
				addToChild(oldEntry.subnode);
			}
		} else {
			setEntry(freeIndex, node);
		}
	}
	
	private int findFreeIndex() {
		for (int i = 0; i < entries.length; i++) {
			if (entries[i] == null) return i;
		}
		return -1;
	}
	
	private int findSubstitutionIndex(Point representative) {
		int[] substitutionRowsums = calculateSubstitutionRowsums(representative);
		int highestDistance = 0;
		int substitutionIndex = -1;
		for (int i=0; i < entries.length; i++) {
			int distance = substitutionRowsums[i] - entries[i].rowsum;
			if (distance > highestDistance) {
				highestDistance = distance;
				substitutionIndex = i;
			}
		}
		return substitutionIndex;
	}

	private int calculateRowsum(int withoutIndex, Point point) {
		int rowsum = 0;
		for (int i=0; i < entries.length; i++) {
			if (withoutIndex == i || entries[i] == null) {
				continue;
			}
			int distance = point.getDistance(entries[i].getPoint());
			rowsum += distance * distance;
		}
		return rowsum;
	}
	
	private int[] calculateSubstitutionRowsums(Point point) {
		int[] rowsums = new int[entries.length];
		for (int i=0; i < entries.length; i++) {
			if (entries[i] == null) {
				rowsums[i] = -1;
			} else {
				rowsums[i] = calculateRowsum(i, point);
			}
		}
		return rowsums;
	}
	
	private void setEntry(int index, INode subnode) {
		if (entries[index] != null) {
			freeEntry(index);
		}
		entries[index] = new Entry(subnode);
		subnode.setParent(this);
		updateRepresentative();
	}
	
	private void freeEntry(int index) {
		entries[index].subnode.setParent(null);
		entries[index] = null;
		if (parent != null && isEmpty()) {
			parent.remove(this);
		} else {
			updateRepresentative();
		}
	}
	
	private boolean isEmpty() {
		return collectSetIndexes().isEmpty();
	}

	/**
	 * Lets nearest child add the given node.
	 * @param node
	 */
	private void addToChild(INode node) {
		INode closestChild = findClosestChild(node.getRepresentantive());
		int index = findIndex(closestChild);
		freeEntry(index);
		InnerNode mergedNode = tree.merge(node, closestChild);
		setEntry(index, mergedNode);
	}
	

	public Collection<INode> getSubnodes() {
		Collection<INode> subnodes = new ArrayList<INode>(entries.length);
		for (Entry entry : entries) {
			if (entry != null) {
				subnodes.add(entry.subnode);
			}
		}
		return subnodes;
	}
	

	private int findIndex(INode node) {
		for (int i = 0; i < entries.length; i++) {
			Entry entry = entries[i];
			if (entry != null && entry.subnode == node) {
				return i;
			}
		}
		throw new IllegalArgumentException("No such subnode");
	}

	private int[][] calculateDistanceMatrix(List<Integer> indexes) {
		return calculateDistanceMatrix(indexes, entries);
	}
	
	private int[][] calculateDistanceMatrix(List<Integer> indexes, Entry[] entries) {
		int[][] distances = new int[indexes.size()][indexes.size()];
		for (int i = 0; i < indexes.size() - 1; i++) {
			Point point1 = entries[indexes.get(i)].getPoint();
			for (int j = i + 1; j < indexes.size(); j++) {
				Point point2 = entries[indexes.get(j)].getPoint();
				distances[i][j] = distances[j][i] = 
						point1.getDistance(point2);
			}
		}
		return distances;
	}

	private List<Integer> collectSetIndexes() {
		return collectSetIndexes(entries);
	}
	
	private List<Integer> collectSetIndexes(Entry[] entries) {
		List<Integer> indexes = new ArrayList<Integer>(entries.length);
		for (int i = 0; i < entries.length; i++) {
			if (entries[i] != null) {
				indexes.add(i);
			}
		}
		return indexes;
	}

	public Collection<Leaf> getLeafs() {
		List<Leaf> leafs = new ArrayList<Leaf>();
		for (Entry entry : entries) {
			if (entry != null) {
				leafs.addAll(entry.subnode.getLeafs());
			}
		}
		return leafs;
	}

	@Override
	public Point getRepresentantive() {
		return representative;
	}
	
	@Override
	public void updateRepresentative() {
		Point oldRepresentative = representative;
		representative = calculateRepresentantive();
		if (parent != null && !representative.equals(oldRepresentative)) {
			parent.updateRepresentative();
		}
	}
	
	private Point calculateRepresentantive() {
		// trivial cases: 0 or 1 entry
		List<Integer> setIndexes = collectSetIndexes();
		if (setIndexes.isEmpty()) {
			return null;
		} else if (setIndexes.size() == 1) {
			return entries[setIndexes.get(0)].getPoint();
		}
		
		// other cases: search point with minimum distance from others
		int[][] distanceMatrix = calculateDistanceMatrix(setIndexes);
		int minSum = -1;
		int candidate = -1;
		for (int i = 0; i < setIndexes.size() - 1; i++) {
			int sum = 0;
			for (int j = i + 1; j < setIndexes.size(); j++) {
				sum += distanceMatrix[i][j];
			}
			if (sum < minSum || minSum < 0) {
				minSum = sum;
				candidate = i;
			}
		}
		return entries[setIndexes.get(candidate)].getPoint();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		String separator = "";
		sb.append("InnerNode[");
		for (Entry entry : entries) {
			if (entry != null) {
				sb.append(separator).append(entry.getPoint());
				separator = ";";
			}
		}
		return sb.append("]").toString();
	}

	public void replace(INode oldNode, INode newNode) {
		int index = findIndex(oldNode);
		setEntry(index, newNode);
	}

	public void remove(INode node) {
		int index = findIndex(node);
		freeEntry(index);
	}

	@Override
	public Leaf findLeafNextTo(Point point) {
		return findClosestChild(point).findLeafNextTo(point);
	}

	private List<Entry> collectSetEntries() {
		List<Integer> setIndexes = collectSetIndexes();
		List<Entry> result = new ArrayList<InnerNode.Entry>(setIndexes.size());
		for (int i : setIndexes) {
			result.add(entries[i]);
		}
		return result;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.sdaa11.JsonSerializable#read(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public void read(IJsonNode node) {
		representative = new Point();
		representative.read(JsonUtil2.getField(node, "representative", ObjectNode.class));
		int i = 0;
		for (IJsonNode member : JsonUtil2.getField(node, "children", IArrayNode.class)) {
			entries[i++] = readEntry(member);
		}
	}
	
	private Entry readEntry(IJsonNode node) {
		int rowsum = JsonUtil2.getField(node, "rowsum", IntNode.class).getIntValue();
		INode subnode;
		if (JsonUtil2.getField(node, "containsLeaf", BooleanNode.class).getBooleanValue()) {
			subnode = tree.createLeaf(null, null);
		} else {
			subnode = tree.createInnerNode();
		}
		subnode.read(JsonUtil2.getField(node, "child", IJsonNode.class));
		return new Entry(subnode, rowsum);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.sdaa11.JsonSerializable#write(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode write(IJsonNode node) {
		ObjectNode objectNode = JsonUtil2.reuseObjectNode(node);
		objectNode.put("representative", representative.write(null));
		objectNode.put("children", writeEntries());
		return objectNode;
	}

	/**
	 * @return
	 */
	private IJsonNode writeEntries() {
		ArrayNode array = new ArrayNode();
		for (Entry entry : collectSetEntries()) {
			array.add(writeEntry(entry, null));
		}
		return array;
	}
	
	private IJsonNode writeEntry(Entry entry, IJsonNode node) {
		ObjectNode objectNode = JsonUtil2.reuseObjectNode(node);
		objectNode.put("rowsum", new IntNode(entry.rowsum));
		objectNode.put("containsLeaf", BooleanNode.valueOf(entry.subnode instanceof Leaf));
		objectNode.put("child", entry.subnode.write(null));
		return objectNode;
	}
	
	
}
