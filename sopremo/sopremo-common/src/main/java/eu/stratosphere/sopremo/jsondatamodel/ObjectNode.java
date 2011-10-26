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
import java.util.TreeMap;

import eu.stratosphere.sopremo.pact.SopremoUtil;

public class ObjectNode extends JsonNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 222657144282059523L;

	/**
	 * Do not store null nodes
	 */
	protected Map<String, JsonNode> children = new TreeMap<String, JsonNode>();

	public int size() {
		return this.children.size();
	}

	public ObjectNode put(final String fieldName, final JsonNode value) {
		if (value == null)
			throw new NullPointerException();

		if (value.isNull())
			children.remove(fieldName);
		else
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
	public StringBuilder toString(final StringBuilder sb) {
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
		// TODO: improve?
		return this.compareTo(other) == 0 ? true : false;
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
			JsonNode node = SopremoUtil.deserializeNode(in);
			final String key = in.readUTF();

			node.read(in);
			this.put(key, node.canonicalize());
		}
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeInt(this.children.size());

		for (final Entry<String, JsonNode> entry : this.children.entrySet()) {
			SopremoUtil.serializeNode(out, entry.getValue());
			out.writeUTF(entry.getKey());
			
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
	public int compareToSameType(final JsonNode other) {

		final ObjectNode node = (ObjectNode) other;
		final Iterator<Entry<String, JsonNode>> entries1 = this.children.entrySet().iterator(), entries2 = node.children
			.entrySet().iterator();

		while (entries1.hasNext() && entries2.hasNext()) {
			Entry<String, JsonNode> entry1 = entries1.next(), entry2 = entries2.next();
			final int keyComparison = entry1.getKey().compareTo(entry2.getKey());
			if (keyComparison != 0)
				return keyComparison;

			final int valueComparison = entry1.getValue().compareTo(node.get(entry1.getKey()));
			if (valueComparison != 0)
				return valueComparison;
		}

		if (!entries1.hasNext())
			return entries2.hasNext() ? -1 : 0;
		if (!entries2.hasNext())
			return 1;
		return 0;
	}

	@Override
	public ObjectNode clone() {
		final ObjectNode clone = (ObjectNode) super.clone();
		clone.children = new LinkedHashMap<String, JsonNode>(this.children);
		return clone;
	}

}
