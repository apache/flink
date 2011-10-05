package eu.stratosphere.sopremo.jsondatamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import eu.stratosphere.pact.common.type.Key;

public class ObjectNode extends JsonNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 222657144282059523L;

	protected LinkedHashMap<String, JsonNode> children = new LinkedHashMap<String, JsonNode>();

	public int size() {
		return this.children.size();
	}

	public ObjectNode put(final String fieldName, final JsonNode value) {
		if (value == null)
			throw new NullPointerException();

		this._put(fieldName, value);
		return this;
	}

	public ObjectNode put(final String fieldName, final String value) {
		if (value == null)
			throw new NullPointerException();

		this._put(fieldName, new TextNode(value));
		return this;
	}

	public ObjectNode put(final String fieldName, final Integer value) {
		if (value == null)
			throw new NullPointerException();

		this._put(fieldName, new IntNode(value));
		return this;
	}

	public ObjectNode put(final String fieldName, final Double value) {
		if (value == null)
			throw new NullPointerException();

		this._put(fieldName, new DoubleNode(value));
		return this;
	}

	public ObjectNode put(final String fieldName, final BigDecimal value) {
		if (value == null)
			throw new NullPointerException();

		this._put(fieldName, new DecimalNode(value));
		return this;
	}

	public ObjectNode put(final String fieldName, final Boolean value) {
		if (value == null)
			throw new NullPointerException();

		this._put(fieldName, value ? BooleanNode.TRUE : BooleanNode.FALSE);
		return this;
	}

	public JsonNode get(final String fieldName) {
		final JsonNode node = this.children.get(fieldName);
		if (node != null)
			return node;
		return NullNode.getInstance();
	}

	public JsonNode remove(final String fieldName) {
		final JsonNode node = this.children.remove(fieldName);
		if (node != null)
			return node;
		return NullNode.getInstance();
	}

	public ObjectNode removeAll() {
		this.children.clear();
		return this;
	}

	@Override
	public StringBuilder toString(StringBuilder sb) {
		sb.append('{');

		int count = 0;
		for (final Map.Entry<String, JsonNode> en : this.children.entrySet()) {
			if (count > 0)
				sb.append(',');
			++count;

			TextNode.appendQuoted(sb, en.getKey());
			sb.append(':');
			en.getValue().toString(sb);
		}

		sb.append('}');
		return sb;
	}

	private JsonNode _put(final String fieldName, final JsonNode value) {
		return this.children.put(fieldName, value);
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

		final ObjectNode other = (ObjectNode) obj;
		if (!this.children.equals(other.children))
			return false;
		return true;
	}

	@Override
	public int getTypePos() {
		return TYPES.ObjectNode.ordinal();
	}

	@Override
	public void read(final DataInput in) throws IOException {
		this.children.clear();
		final int len = in.readInt();

		for (int i = 0; i < len; i++) {
			JsonNode node;
			final String key = in.readUTF();

			try {
				node = TYPES.values()[in.readInt()].getClazz().newInstance();
				node.read(in);
				this.put(key, node.canonicalize());
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

		for (final Entry<String, JsonNode> entry : this.children.entrySet()) {
			out.writeUTF(entry.getKey());
			out.writeInt(entry.getValue().getTypePos());
			entry.getValue().write(out);
		}

	}

	public Set<Entry<String, JsonNode>> getEntries() {
		return this.children.entrySet();
	}

	public ObjectNode putAll(final ObjectNode jsonNode) {
		for (final Entry<String, JsonNode> entry : jsonNode.getEntries())
			this.put(entry.getKey(), entry.getValue());
		return this;
	}

	@Override
	public boolean isObject() {
		return true;
	}

	public Iterator<String> getFieldNames() {
		return this.children.keySet().iterator();
	}

	public Iterator<Entry<String, JsonNode>> getFields() {
		return this.children.entrySet().iterator();
	}

	@Override
	public TYPES getType() {
		return TYPES.ObjectNode;
	}

	@Override
	public int compareTo(Key other) {

		ObjectNode node = (ObjectNode) other;
		if (node.size() != this.size()) {
			return 1;
		}
		for (Entry<String, JsonNode> entry : this.children.entrySet()) {
			int comparison = entry.getValue().compareTo(node.get(entry.getKey()));
			if (comparison != 0) {
				return comparison;
			}
		}
		return 0;
	}

	public ObjectNode clone() {
		ObjectNode clone = (ObjectNode) super.clone();
		clone.children = new LinkedHashMap<String, JsonNode>(children);
		return clone;
	}

}
