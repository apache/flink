package eu.stratosphere.sopremo.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import eu.stratosphere.sopremo.pact.SopremoUtil;

public class ArrayNode extends JsonNode implements IArrayNode {
	/**
	 * 
	 */
	private static final long serialVersionUID = 898220542834090837L;

	private List<IJsonNode> children = new ArrayList<IJsonNode>();

	/**
	 * Initializes an empty ArrayNode.
	 */
	public ArrayNode() {
	}

	/**
	 * Initializes an ArrayNode which contains the given {@link IJsonNode}s in proper sequence.
	 *
	 * @param nodes
	 * 	the nodes which should be added to this ArrayNode
	 */
	public ArrayNode(final IJsonNode... nodes) {
		for (final IJsonNode node : nodes)
			this.add(node);
	}

	/**
	 * Initializes an ArrayNode which cointains all {@link IJsonNode}s from the given Collection in proper sequence.
	 *
	 * @param nodes
	 *  a Collection of nodes which should be added to this ArrayNode
	 */
	public ArrayNode(final Collection<? extends IJsonNode> nodes) {
		for (final IJsonNode node : nodes)
			this.add(node);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonArray#size()
	 */
	@Override
	public int size() {
		return this.children.size();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonArray#add(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public ArrayNode add(final IJsonNode node) {
		if (node == null)
			throw new NullPointerException();
		
		if(!node.isMissing()){
			this.children.add(node);
		}
		
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonArray#add(int, eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IArrayNode add(final int index, final IJsonNode element) {
		if (element == null)
			throw new NullPointerException();
		
		if(element.isMissing()){
			this.children.remove(index);
		} else {
			this.children.add(index, element);
		}
		
		return this;
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
	public StringBuilder toString(final StringBuilder sb) {
		sb.append('[');

		for (int i = 0; i < this.children.size(); i++) {
			if (i > 0)
				sb.append(',');
			this.children.get(i).toString(sb);
		}

		sb.append(']');
		return sb;
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
	public void read(final DataInput in) throws IOException {
		this.children.clear();
		final int len = in.readInt();

		for (int i = 0; i < len; i++) {
			IJsonNode node;
			try {
				node = Type.values()[in.readInt()].getClazz().newInstance();
				node.read(in);
				this.add(node.canonicalize());
			} catch (final InstantiationException e) {
				e.printStackTrace();
			} catch (final IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public List<IJsonNode> getJavaValue() {
		return this.children;
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeInt(this.children.size());

		for (final IJsonNode child : this.children) {
			SopremoUtil.serializeNode(out, child);
		}
	}

	@Override
	public ArrayNode clone() {
		final ArrayNode clone = (ArrayNode) super.clone();
		clone.children = new ArrayList<IJsonNode>(this.children);
		final ListIterator<IJsonNode> listIterator = clone.children.listIterator();
		while (listIterator.hasNext())
			listIterator.set(listIterator.next().clone());
		return clone;
	}

	@Override
	public Iterator<IJsonNode> iterator() {
		return this.children.iterator();
	}

	@Override
	public boolean isArray() {
		return true;
	}

	/**
	 * Checks if this node is currently empty.
	 */
	public boolean isEmpty() {
		return this.children.isEmpty();
	}

	
	public static ArrayNode valueOf(final Iterator<IJsonNode> iterator) {
		final ArrayNode array = new ArrayNode();
		while (iterator.hasNext())
			array.add(iterator.next());
		return array;
	}

	public IJsonNode[] toArray() {
		return this.children.toArray(new IJsonNode[this.children.size()]);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonArray#addAll(java.util.Collection)
	 */
	@Override
	public IArrayNode addAll(final Collection<? extends IJsonNode> c) {
		for (final IJsonNode jsonNode : c)
			this.add(jsonNode);
		return this;
	}

	@Override
	public Type getType() {
		return Type.ArrayNode;
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

	public IJsonNode subArray(final int fromIndex, final int toIndex) {
		return new ArrayNode(this.children.subList(fromIndex, toIndex));
	}

	@Override
	public IArrayNode addAll(IArrayNode node) {
		for (IJsonNode n : node) {
			this.add(n);
		}
		return this;
	}

}
