package eu.stratosphere.sopremo.type;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * This node represents a json object.
 */
public class ObjectNode extends AbstractObjectNode implements IObjectNode {

	/**
	 * @author Michael Hopstock
	 * @author Tommy Neubert
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

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#put(java.lang.String, eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public ObjectNode put(final String fieldName, final IJsonNode value) {
		if (value == null)
			throw new NullPointerException();

		if (value.isMissing())
			this.children.remove(fieldName);
		else
			this.children.put(fieldName, value);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#get(java.lang.String)
	 */
	@Override
	public IJsonNode get(final String fieldName) {
		final IJsonNode node = this.children.get(fieldName);
		if (node != null)
			return node;
		return MissingNode.getInstance();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#remove(java.lang.String)
	 */
	@Override
	public IJsonNode remove(final String fieldName) {
		final IJsonNode node = this.children.remove(fieldName);
		if (node != null)
			return node;
		return MissingNode.getInstance();
	}

	@Override
	public void toString(final StringBuilder sb) {
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
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.children.hashCode();
		return result;
	}

	@Override
	public Iterator<Entry<String, IJsonNode>> iterator() {
		return this.children.entrySet().iterator();
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
		return this.compareTo(other) == 0;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#putAll(eu.stratosphere.sopremo.type.JsonObject)
	 */
	@Override
	public IObjectNode putAll(final IObjectNode jsonNode) {
		for (final Entry<String, IJsonNode> entry : jsonNode)
			this.put(entry.getKey(), entry.getValue());
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#getFieldNames()
	 */
	@Override
	public Iterator<String> getFieldNames() {
		return this.children.keySet().iterator();
	}

	@Override
	public int compareToSameType(final IJsonNode other) {

		final ObjectNode node = (ObjectNode) other;
		final Iterator<Entry<String, IJsonNode>> entries1 = this.children.entrySet().iterator(), entries2 =
			node.children
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
	public void clear() {
		this.children.clear();
	}
}
