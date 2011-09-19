package eu.stratosphere.sopremo.jsondatamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class ArrayNode extends JsonNode implements Iterable<JsonNode> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 898220542834090837L;

	protected ArrayList<JsonNode> children = new ArrayList<JsonNode>();

	public ArrayNode() {
	}

	public ArrayNode(final JsonNode... nodes) {
		for (final JsonNode node : nodes)
			this._add(node);
	}

	public int size() {
		return this.children.size();
	}

	public void add(JsonNode node) {
		if (node == null)
			node = NullNode.getInstance();
		this._add(node);
	}

	public JsonNode get(final int index) {
		if (index >= 0 && index < this.children.size())
			return this.children.get(index);
		return NullNode.getInstance();
	}

	public JsonNode set(final int index, JsonNode node) {
		if (node == null)
			node = NullNode.getInstance();
		return this._set(index, node);
	}

	public JsonNode remove(final int index) {
		if (index >= 0 && index < this.children.size())
			return this.children.remove(index);
		return NullNode.getInstance();
	}

	public ArrayNode removeAll() {
		this.children.clear();
		return this;
	}

	private void _add(final JsonNode node) {
		this.children.add(node);
	}

	private JsonNode _set(final int index, final JsonNode node) {
		if (index < 0 || index >= this.children.size())
			throw new IndexOutOfBoundsException("Illegal index " + index + ", array size " + this.children.size());
		return this.children.set(index, node);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(16 + (this.size() << 4));
		sb.append('[');

		for (int i = 0; i < this.children.size(); i++) {
			if (i > 0)
				sb.append(',');
			sb.append(this.children.get(i).toString());
		}

		sb.append(']');
		return sb.toString();
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
	public int getTypePos() {
		return TYPES.ArrayNode.ordinal();
	}

	@Override
	public void read(final DataInput in) throws IOException {
		final int len = in.readInt();

		for (int i = 0; i < len; i++) {
			JsonNode node;
			try {
				node = TYPES.values()[in.readInt()].getClazz().newInstance();
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
	public void write(final DataOutput out) throws IOException {
		out.writeInt(this.children.size());

		for (final JsonNode child : this.children) {

			out.writeInt(child.getTypePos());
			child.write(out);
		}
	}

	@Override
	public Iterator<JsonNode> iterator() {
		return this.children.iterator();
	}

	@Override
	public boolean isArray() {
		return true;
	}

	public ArrayNode addAll(final Collection<JsonNode> values) {
		for (final JsonNode node : values)
			this.add(node);
		return this;
	}

	public boolean isEmpty() {
		return this.children.isEmpty();
	}

	 public static ArrayNode valueOf(Iterator<JsonNode> iterator) {
	 ArrayNode array = new ArrayNode();
	 while(iterator.hasNext())
	 {
	 array.add(iterator.next());
	 }
	 return array;
	 }
	 
	 public JsonNode[] toArray(){
		 return this.children.toArray(new JsonNode[this.children.size()]);
	 }

	@Override
	public TYPES getType() {
		return TYPES.ArrayNode;
	}

}
