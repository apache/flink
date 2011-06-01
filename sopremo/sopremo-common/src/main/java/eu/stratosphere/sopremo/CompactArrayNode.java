package eu.stratosphere.sopremo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.node.BaseJsonNode;
import org.codehaus.jackson.node.ContainerNode;
import org.codehaus.jackson.node.NullNode;
import org.codehaus.jackson.node.ObjectNode;

import eu.stratosphere.util.AbstractIterator;

/**
 * Read-only array node wrapping a java array of {@link JsonNode}.
 * 
 * @author Arvid Heise
 */
public class CompactArrayNode extends ContainerNode {
	private JsonNode[] children;

	/**
	 * Initializes CompactArrayNode with the given {@link JsonNode}s as children.
	 * 
	 * @param children
	 *        the child nodes to wrap
	 */
	public CompactArrayNode(JsonNode[] children) {
		super(JsonUtil.NODE_FACTORY);
		this.children = children;
	}

	@Override
	public JsonToken asToken() {
		return null;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		CompactArrayNode other = (CompactArrayNode) obj;
		if (!Arrays.equals(this.children, other.children))
			return false;
		return true;
	}

	@Override
	public ObjectNode findParent(String fieldName) {
		return null;
	}

	@Override
	public List<JsonNode> findParents(String fieldName, List<JsonNode> foundSoFar) {
		return null;
	}

	@Override
	public JsonNode findValue(String fieldName) {
		return null;
	}

	@Override
	public List<JsonNode> findValues(String fieldName, List<JsonNode> foundSoFar) {
		return null;
	}

	@Override
	public List<String> findValuesAsText(String fieldName, List<String> foundSoFar) {
		return null;
	}

	@Override
	public JsonNode get(int index) {
		return this.children[index];
	}

	@Override
	public JsonNode get(String fieldName) {
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
	public JsonNode path(int index) {
		return this.children[index];
	}

	@Override
	public JsonNode path(String fieldName) {
		return null;
	}

	@Override
	public ContainerNode removeAll() {
		return null;
	}

	@Override
	public void serialize(JsonGenerator jg, SerializerProvider provider) throws IOException, JsonProcessingException {
		jg.writeStartArray();
		for (JsonNode n : this.children) {
			if (n == null)
				NullNode.instance.writeTo(jg);
			else
				((BaseJsonNode) n).writeTo(jg);
		}
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
