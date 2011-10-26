package eu.stratosphere.sopremo.jsondatamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import eu.stratosphere.sopremo.pact.SopremoUtil;

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
			this.add(node);
	}

	public int size() {
		return this.children.size();
	}

	public ArrayNode add(JsonNode node) {
		if (node == null)
			node = NullNode.getInstance();
		this._add(node);
		return this;
	}

	public void add(int index, JsonNode element) {
		children.add(index, element);
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
	public int getTypePos() {
		return TYPES.ArrayNode.ordinal();
	}

	@Override
	public void read(final DataInput in) throws IOException {
		this.children.clear();
		final int len = in.readInt();

		for (int i = 0; i < len; i++) {
			JsonNode node = SopremoUtil.deserializeNode(in);
			node.read(in);
			this.add(node.canonicalize());
		}
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeInt(this.children.size());

		for (final JsonNode child : this.children) {
			SopremoUtil.serializeNode(out, child);
			child.write(out);
		}
	}

	@Override
	public ArrayNode clone() {
		final ArrayNode clone = (ArrayNode) super.clone();
		clone.children = new ArrayList<JsonNode>(this.children);
		return clone;
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

	public static ArrayNode valueOf(final Iterator<JsonNode> iterator) {
		final ArrayNode array = new ArrayNode();
		while (iterator.hasNext())
			array.add(iterator.next());
		return array;
	}

	public JsonNode[] toArray() {
		return this.children.toArray(new JsonNode[this.children.size()]);
	}

	@Override
	public TYPES getType() {
		return TYPES.ArrayNode;
	}

	@Override
	public int compareToSameType(final JsonNode other) {
		// if(!(other instanceof ArrayNode)){
		// return -1;
		// }
		final ArrayNode node = (ArrayNode) other;
		if (node.size() != this.size())
			return this.size() - node.size();
		for (int i = 0; i < this.size(); i++) {
			final int comp = this.get(i).compareTo(node.get(i));
			if (comp != 0)
				return comp;
		}
		return 0;
	}

}
