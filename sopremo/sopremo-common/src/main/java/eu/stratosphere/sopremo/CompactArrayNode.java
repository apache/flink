package eu.stratosphere.sopremo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.jsondatamodel.ObjectNode;
import eu.stratosphere.util.AbstractIterator;

/**
 * Read-only array node wrapping a java array of {@link JsonNode}.
 * 
 * @author Arvid Heise
 */
public class CompactArrayNode extends ContainerNode {
	private final JsonNode[] children;

	/**
	 * Initializes CompactArrayNode with the given {@link JsonNode}s as children.
	 * 
	 * @param children
	 *        the child nodes to wrap
	 */
	public CompactArrayNode(final JsonNode[] children) {
		super(JsonUtil.NODE_FACTORY);
		this.children = children;
	}

	@Override
	public JsonToken asToken() {
		return null;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final CompactArrayNode other = (CompactArrayNode) obj;
		return Arrays.equals(this.children, other.children);
	}

	@Override
	public ObjectNode findParent(final String fieldName) {
		return null;
	}

	@Override
	public List<JsonNode> findParents(final String fieldName, final List<JsonNode> foundSoFar) {
		return null;
	}

	@Override
	public JsonNode findValue(final String fieldName) {
		return null;
	}

	@Override
	public List<JsonNode> findValues(final String fieldName, final List<JsonNode> foundSoFar) {
		return null;
	}

	@Override
	public List<String> findValuesAsText(final String fieldName, final List<String> foundSoFar) {
		return null;
	}

	@Override
	public JsonNode get(final int index) {
		return this.children[index];
	}

	@Override
	public JsonNode get(final String fieldName) {
		return null;
	}

	/**
	 * Returns the backing array of the children.
	 * 
	 * @return the children
	 */
	public JsonNode[] getChildren() {
		return this.children;
	}

	@Override
	public Iterator<JsonNode> getElements() {
		return new AbstractIterator<JsonNode>() {
			int index = 0;

			@Override
			protected JsonNode loadNext() {
				if (this.index < CompactArrayNode.this.children.length)
					return CompactArrayNode.this.children[this.index++];
				return this.noMoreElements();
			}
		};
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(this.children);
		return result;
	}

	@Override
	public boolean isArray() {
		return true;
	}

	@Override
	public JsonNode path(final int index) {
		return this.children[index];
	}

	@Override
	public JsonNode path(final String fieldName) {
		return null;
	}

	@Override
	public ContainerNode removeAll() {
		return null;
	}

	@Override
	public void serialize(final JsonGenerator jg, final SerializerProvider provider) throws IOException,
			JsonProcessingException {
		jg.writeStartArray();
		for (final JsonNode n : this.children)
			if (n == null)
				NullNode.instance.writeTo(jg);
			else
				((BaseJsonNode) n).writeTo(jg);
		jg.writeEndArray();
	}

	@Override
	public int size() {
		return this.children.length;
	}

	@Override
	public String toString() {
		return Arrays.toString(this.children);
	}

}
