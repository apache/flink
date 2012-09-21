package eu.stratosphere.sopremo.type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.util.CollectionUtil;

/**
 * This node represents an array and can store all types of {@link IJsonNode}s. In addition, the size of the array
 * increases when needed.
 * 
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public class ArrayNode extends AbstractArrayNode {
	/**
	 * 
	 */
	private static final long serialVersionUID = 898220542834090837L;

	private final List<IJsonNode> children;

	/**
	 * Initializes an empty ArrayNode.
	 */
	public ArrayNode() {
		this(new ArrayList<IJsonNode>());
	}

	/**
	 * Initializes an empty ArrayNode directly with the given list.
	 */
	protected ArrayNode(List<IJsonNode> children) {
		this.children = children;
	}

	/**
	 * Initializes an ArrayNode which contains the given {@link IJsonNode}s in proper sequence.
	 * 
	 * @param nodes
	 *        the nodes that should be added to this ArrayNode
	 */
	public ArrayNode(final IJsonNode... nodes) {
		this();
		for (final IJsonNode node : nodes)
			this.children.add(node);
	}

	/**
	 * Initializes an ArrayNode which cointains all {@link IJsonNode}s from the given Collection in proper sequence.
	 * 
	 * @param nodes
	 *        a Collection of nodes that should be added to this ArrayNode
	 */
	public ArrayNode(final Collection<? extends IJsonNode> nodes) {
		this();
		for (final IJsonNode node : nodes)
			this.children.add(node);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#asCollection()
	 */
	@Override
	public Collection<IJsonNode> asCollection() {
		return this.children;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonArray#size()
	 */
	@Override
	public int size() {
		int size = this.children.size();
		while (size > 0 && this.children.get(size - 1).isMissing())
			size--;
		return size;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonArray#add(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public ArrayNode add(final IJsonNode node) {
		if (node == null)
			throw new NullPointerException();

		this.children.add(node);

		return this;
	}

	/**
	 * Returns the children.
	 * 
	 * @return the children
	 */
	protected List<IJsonNode> getChildren() {
		return this.children;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonArray#add(int, eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IArrayNode add(final int index, final IJsonNode element) {
		if (element == null)
			throw new NullPointerException();

		this.children.add(index, element);

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.AbstractArrayNode#contains(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public boolean contains(IJsonNode node) {
		return this.children.contains(node);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonArray#get(int)
	 */
	@Override
	public IJsonNode get(final int index) {
		if (0 <= index && index < this.children.size())
			return this.children.get(index);
		return MissingNode.getInstance();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonArray#set(int, eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode set(final int index, final IJsonNode node) {
		if (node == null)
			throw new NullPointerException();
		CollectionUtil.ensureSize(this.children, index + 1, MissingNode.getInstance());
		return this.children.set(index, node);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonArray#remove(int)
	 */
	@Override
	public IJsonNode remove(final int index) {
		if (0 <= index && index < this.children.size())
			return this.children.remove(index);
		// throw new ArrayIndexOutOfBoundsException();
		return MissingNode.getInstance();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonArray#clear()
	 */
	@Override
	public void clear() {
		this.children.clear();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.children.hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;

		final ArrayNode other = (ArrayNode) obj;
		if (!this.children.equals(other.children))
			return false;
		return true;
	}

	@Override
	public List<IJsonNode> getJavaValue() {
		return this.children;
	}

	@Override
	public Iterator<IJsonNode> iterator() {
		return this.children.iterator();
	}

	/**
	 * Checks if this node is currently empty.
	 */
	@Override
	public boolean isEmpty() {
		return this.children.isEmpty();
	}

	/**
	 * Initializes a new ArrayNode which contains all {@link IJsonNode}s from the provided Iterator.
	 * 
	 * @param iterator
	 *        an Iterator over IJsonNodes that should be added to the new ArrayNode
	 * @return the created ArrayNode
	 */
	public static ArrayNode valueOf(final Iterator<IJsonNode> iterator) {
		final ArrayNode array = new ArrayNode();
		while (iterator.hasNext())
			array.add(iterator.next());
		return array;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.AbstractArrayNode#fillArray(eu.stratosphere.sopremo.type.IJsonNode[])
	 */
	@Override
	protected void fillArray(IJsonNode[] result) {
		IJsonNode[] array = this.children.toArray(new IJsonNode[this.children.size()]);
		for (int i = 0; i < this.children.size(); i++) {
			result[i] = array[i];
		}
	}

	@Override
	public int compareToSameType(final IJsonNode other) {
		// if(!(other instanceof ArrayNode)){
		// return -1;
		// }
		final IArrayNode node = (IArrayNode) other;
		if (node.size() != this.size())
			return this.size() - node.size();
		for (int i = 0; i < this.size(); i++) {
			final int comp = this.get(i).compareTo(node.get(i));
			if (comp != 0)
				return comp;
		}
		return 0;
	}

	/**
	 * Returns a view of the portion of this ArrayNode between the specified fromIndex, inclusive, and toIndex,
	 * exclusive.
	 * (If fromIndex and toIndex are equal, the returned ArrayNode is empty.)
	 * 
	 * @param fromIndex
	 *        the index where the new ArrayNode should start (inclusive)
	 * @param toIndex
	 *        the index where the new ArrayNode should stop (exclusive)
	 * @return the new ArrayNode (subarray)
	 */
	public IJsonNode subArray(final int fromIndex, final int toIndex) {
		return new ArrayNode(this.children.subList(fromIndex, toIndex));
	}
}
