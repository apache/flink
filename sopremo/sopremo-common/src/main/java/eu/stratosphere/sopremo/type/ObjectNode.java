package eu.stratosphere.sopremo.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import eu.stratosphere.sopremo.pact.SopremoUtil;

public class ObjectNode extends JsonNode implements IObjectNode {

	/**
	 * @author Michael Hopstock
	 * @author Tommy Neubert
	 *
	 */
	private static final long serialVersionUID = 222657144282059523L;

	/**
	 * Do not store null nodes
	 */
	protected Map<String, IJsonNode> children = new TreeMap<String, IJsonNode>();

	@Override
	public int size() {
		return this.children.size();
	}

	@Override
	public Map<String, IJsonNode> getJavaValue() {
		return this.children;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#put(java.lang.String, eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public ObjectNode put(final String fieldName, final IJsonNode value) {
		if (value == null)
			throw new NullPointerException();

		if (value.isNull())
			this.children.remove(fieldName);
		else
			this.children.put(fieldName, value);
		return this;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#get(java.lang.String)
	 */
	@Override
	public IJsonNode get(final String fieldName) {
		final IJsonNode node = this.children.get(fieldName);
		if (node != null)
			return node;
		return NullNode.getInstance();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#remove(java.lang.String)
	 */
	@Override
	public IJsonNode remove(final String fieldName) {
		final IJsonNode node = this.children.remove(fieldName);
		if (node != null)
			return node;
		return NullNode.getInstance();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#removeAll()
	 */
	@Override
	public IObjectNode removeAll() {
		this.children.clear();
		return this;
	}

	@Override
	public StringBuilder toString(final StringBuilder sb) {
		sb.append('{');

		int count = 0;
		for (final Map.Entry<String, IJsonNode> en : this.children.entrySet()) {
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
	public void read(final DataInput in) throws IOException {
		this.children.clear();
		final int len = in.readInt();

		for (int i = 0; i < len; i++) {
			
			final String key = in.readUTF();
			IJsonNode node = SopremoUtil.deserializeNode(in);
			this.put(key, node.canonicalize());
		}
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeInt(this.children.size());

		for (final Entry<String, IJsonNode> entry : this.children.entrySet()) {
			out.writeUTF(entry.getKey());
			SopremoUtil.serializeNode(out, entry.getValue());
		}

	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#getEntries()
	 */
	@Override
	public Set<Entry<String, IJsonNode>> getEntries() {
		return this.children.entrySet();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#putAll(eu.stratosphere.sopremo.type.JsonObject)
	 */
	@Override
	public IObjectNode putAll(final IObjectNode jsonNode) {
		for (final Entry<String, IJsonNode> entry : jsonNode.getEntries())
			this.put(entry.getKey(), entry.getValue());
		return this;
	}

	@Override
	public boolean isObject() {
		return true;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#getFieldNames()
	 */
	@Override
	public Iterator<String> getFieldNames() {
		return this.children.keySet().iterator();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#iterator()
	 */
	@Override
	public Iterator<Entry<String, IJsonNode>> iterator() {
		return this.children.entrySet().iterator();
	}

	@Override
	public Type getType() {
		return Type.ObjectNode;
	}

	@Override
	public int compareToSameType(final IJsonNode other) {

		final ObjectNode node = (ObjectNode) other;
		final Iterator<Entry<String, IJsonNode>> entries1 = this.children.entrySet().iterator(), entries2 = node.children
			.entrySet().iterator();

		while (entries1.hasNext() && entries2.hasNext()) {
			final Entry<String, IJsonNode> entry1 = entries1.next(), entry2 = entries2.next();
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
		clone.children = new LinkedHashMap<String, IJsonNode>(this.children);
		for (final Entry<String, IJsonNode> entry : clone.children.entrySet())
			entry.setValue(entry.getValue().clone());
		return clone;
	}
}
