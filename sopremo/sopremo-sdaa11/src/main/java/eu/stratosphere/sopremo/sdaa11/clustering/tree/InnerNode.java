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

		private Entry(final INode subnode) {
			this(subnode, -1);
		}

		private Entry(final INode subnode, final int rowsum) {
			this.subnode = subnode;
		}

		private final INode subnode;
		private int rowsum;

		private Point getPoint() {
			return this.subnode.getRepresentantive();
		}

	}

	private final Entry[] entries;
	private Point representative;

	public InnerNode(final ClusterTree tree, final int degree) {
		super(tree);
		this.entries = new Entry[degree];
	}

	private INode findClosestChild(final Point point) {
		int minDistance = -1;
		INode subnode = null;
		for (final Entry entry : this.entries)
			if (entry != null) {
				final int tempDistance = point.getDistance(entry.getPoint());
				if (minDistance < 0 || tempDistance < minDistance) {
					minDistance = tempDistance;
					subnode = entry.subnode;
				}
			}
		if (subnode == null)
			throw new IllegalStateException("Did not find a subnode");
		return subnode;
	}

	@Override
	public void add(final INode node) {
		final int freeIndex = this.findFreeIndex();
		if (freeIndex < 0) {
			final int substitutionIndex = this.findSubstitutionIndex(node
					.getRepresentantive());
			if (substitutionIndex == -1)
				this.addToChild(node);
			else {
				final Entry oldEntry = this.entries[substitutionIndex];
				this.setEntry(substitutionIndex, node);
				this.addToChild(oldEntry.subnode);
			}
		} else
			this.setEntry(freeIndex, node);
	}

	private int findFreeIndex() {
		for (int i = 0; i < this.entries.length; i++)
			if (this.entries[i] == null)
				return i;
		return -1;
	}

	private int findSubstitutionIndex(final Point representative) {
		final int[] substitutionRowsums = this
				.calculateSubstitutionRowsums(representative);
		int highestDistance = 0;
		int substitutionIndex = -1;
		for (int i = 0; i < this.entries.length; i++) {
			final int distance = substitutionRowsums[i]
					- this.entries[i].rowsum;
			if (distance > highestDistance) {
				highestDistance = distance;
				substitutionIndex = i;
			}
		}
		return substitutionIndex;
	}

	private int calculateRowsum(final int withoutIndex, final Point point) {
		int rowsum = 0;
		for (int i = 0; i < this.entries.length; i++) {
			if (withoutIndex == i || this.entries[i] == null)
				continue;
			final int distance = point.getDistance(this.entries[i].getPoint());
			rowsum += distance * distance;
		}
		return rowsum;
	}

	private int[] calculateSubstitutionRowsums(final Point point) {
		final int[] rowsums = new int[this.entries.length];
		for (int i = 0; i < this.entries.length; i++)
			if (this.entries[i] == null)
				rowsums[i] = -1;
			else
				rowsums[i] = this.calculateRowsum(i, point);
		return rowsums;
	}

	private void setEntry(final int index, final INode subnode) {
		if (this.entries[index] != null)
			this.freeEntry(index);
		this.entries[index] = new Entry(subnode);
		subnode.setParent(this);
		this.updateRepresentative();
	}

	private void freeEntry(final int index) {
		this.entries[index].subnode.setParent(null);
		this.entries[index] = null;
		if (this.parent != null && this.isEmpty())
			this.parent.remove(this);
		else
			this.updateRepresentative();
	}

	private boolean isEmpty() {
		return this.collectSetIndexes().isEmpty();
	}

	/**
	 * Lets nearest child add the given node.
	 * 
	 * @param node
	 */
	private void addToChild(final INode node) {
		final INode closestChild = this.findClosestChild(node
				.getRepresentantive());
		final int index = this.findIndex(closestChild);
		this.freeEntry(index);
		final AbstractNode mergedNode = this.tree.merge(node, closestChild);
		this.setEntry(index, mergedNode);
	}

	public Collection<INode> getSubnodes() {
		final Collection<INode> subnodes = new ArrayList<INode>(
				this.entries.length);
		for (final Entry entry : this.entries)
			if (entry != null)
				subnodes.add(entry.subnode);
		return subnodes;
	}

	private int findIndex(final INode node) {
		for (int i = 0; i < this.entries.length; i++) {
			final Entry entry = this.entries[i];
			if (entry != null && entry.subnode == node)
				return i;
		}
		throw new IllegalArgumentException("No such subnode");
	}

	private int[][] calculateDistanceMatrix(final List<Integer> indexes) {
		return this.calculateDistanceMatrix(indexes, this.entries);
	}

	private int[][] calculateDistanceMatrix(final List<Integer> indexes,
			final Entry[] entries) {
		final int[][] distances = new int[indexes.size()][indexes.size()];
		for (int i = 0; i < indexes.size() - 1; i++) {
			final Point point1 = entries[indexes.get(i)].getPoint();
			for (int j = i + 1; j < indexes.size(); j++) {
				final Point point2 = entries[indexes.get(j)].getPoint();
				distances[i][j] = distances[j][i] = point1.getDistance(point2);
			}
		}
		return distances;
	}

	private List<Integer> collectSetIndexes() {
		return this.collectSetIndexes(this.entries);
	}

	private List<Integer> collectSetIndexes(final Entry[] entries) {
		final List<Integer> indexes = new ArrayList<Integer>(entries.length);
		for (int i = 0; i < entries.length; i++)
			if (entries[i] != null)
				indexes.add(i);
		return indexes;
	}

	@Override
	public Collection<Leaf> getLeafs() {
		final List<Leaf> leafs = new ArrayList<Leaf>();
		for (final Entry entry : this.entries)
			if (entry != null)
				leafs.addAll(entry.subnode.getLeafs());
		return leafs;
	}

	@Override
	public Point getRepresentantive() {
		return this.representative;
	}

	@Override
	public void updateRepresentative() {
		final Point oldRepresentative = this.representative;
		this.representative = this.calculateRepresentantive();
		if (this.parent != null
				&& !this.representative.equals(oldRepresentative))
			this.parent.updateRepresentative();
	}

	private Point calculateRepresentantive() {
		// trivial cases: 0 or 1 entry
		final List<Integer> setIndexes = this.collectSetIndexes();
		if (setIndexes.isEmpty())
			return null;
		else if (setIndexes.size() == 1)
			return this.entries[setIndexes.get(0)].getPoint();

		// other cases: search point with minimum distance from others
		final int[][] distanceMatrix = this.calculateDistanceMatrix(setIndexes);
		int minSum = -1;
		int candidate = -1;
		for (int i = 0; i < setIndexes.size() - 1; i++) {
			int sum = 0;
			for (int j = i + 1; j < setIndexes.size(); j++)
				sum += distanceMatrix[i][j];
			if (sum < minSum || minSum < 0) {
				minSum = sum;
				candidate = i;
			}
		}
		return this.entries[setIndexes.get(candidate)].getPoint();
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		String separator = "";
		sb.append("InnerNode[");
		for (final Entry entry : this.entries)
			if (entry != null) {
				sb.append(separator).append(entry.getPoint());
				separator = ";";
			}
		return sb.append("]").toString();
	}

	public void replace(final INode oldNode, final INode newNode) {
		final int index = this.findIndex(oldNode);
		this.setEntry(index, newNode);
	}

	public void remove(final INode node) {
		final int index = this.findIndex(node);
		this.freeEntry(index);
	}

	@Override
	public Leaf findLeafNextTo(final Point point) {
		return this.findClosestChild(point).findLeafNextTo(point);
	}

	private List<Entry> collectSetEntries() {
		final List<Integer> setIndexes = this.collectSetIndexes();
		final List<Entry> result = new ArrayList<InnerNode.Entry>(
				setIndexes.size());
		for (final int i : setIndexes)
			result.add(this.entries[i]);
		return result;
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
		this.representative = new Point();
		this.representative.read(JsonUtil2.getField(node, JSON_KEY_REPRESENTATIVE,
				ObjectNode.class));
		int i = 0;
		for (final IJsonNode member : JsonUtil2.getField(node, JSON_KEY_CHILDREN,
				IArrayNode.class))
			this.entries[i++] = this.readEntry(member);
	}

	private Entry readEntry(final IJsonNode node) {
		final int rowsum = JsonUtil2.getField(node, JSON_KEY_ROWSUM, IntNode.class)
				.getIntValue();
		INode subnode;
		if (JsonUtil2.getField(node, JSON_KEY_HAS_LEAF, BooleanNode.class)
				.getBooleanValue())
			subnode = this.tree.createLeaf(null, null);
		else
			subnode = this.tree.createInnerNode();
		subnode.read(JsonUtil2.getField(node, JSON_KEY_CHILD, IJsonNode.class));
		return new Entry(subnode, rowsum);
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
		objectNode.put(JSON_KEY_REPRESENTATIVE, this.representative.write(null));
		objectNode.put(JSON_KEY_CHILDREN, this.writeEntries());
		return objectNode;
	}

	/**
	 * @return
	 */
	private IJsonNode writeEntries() {
		final ArrayNode array = new ArrayNode();
		for (final Entry entry : this.collectSetEntries())
			array.add(this.writeEntry(entry, null));
		return array;
	}

	private IJsonNode writeEntry(final Entry entry, final IJsonNode node) {
		final ObjectNode objectNode = JsonUtil2.reuseObjectNode(node);
		objectNode.put(JSON_KEY_ROWSUM, new IntNode(entry.rowsum));
		objectNode.put(JSON_KEY_HAS_LEAF,
				BooleanNode.valueOf(entry.subnode instanceof Leaf));
		objectNode.put(JSON_KEY_CHILD, entry.subnode.write(null));
		return objectNode;
	}

}
