package eu.stratosphere.sopremo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.JsonParser.NumberType;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.node.ContainerNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

public class CompactArrayNode extends ContainerNode {
	private JsonNode[] children;

	public CompactArrayNode(JsonNode[] children) {
		super(JsonUtils.NODE_FACTORY);
		this.children = children;
	}

	@Override
	public JsonToken asToken() {
		return null;
	}

	@Override
	public JsonNode findValue(String fieldName) {
		return null;
	}

	@Override
	public ObjectNode findParent(String fieldName) {
		return null;
	}

	@Override
	public List<JsonNode> findValues(String fieldName, List<JsonNode> foundSoFar) {
		return null;
	}

	@Override
	public List<JsonNode> findParents(String fieldName, List<JsonNode> foundSoFar) {
		return null;
	}

	@Override
	public boolean isArray() {
		return true;
	}
	
	public JsonNode[] getChildren() {
		return children;
	}
	
	@Override
	public List<String> findValuesAsText(String fieldName, List<String> foundSoFar) {
		return null;
	}

	@Override
	public int size() {
		return children.length;
	}

	@Override
	public JsonNode get(int index) {
		return children[index];
	}

	@Override
	public JsonNode get(String fieldName) {
		return null;
	}

	@Override
	public ContainerNode removeAll() {
		return null;
	}

	@Override
	public Iterator<JsonNode> getElements() {
		return new AbstractIterator<JsonNode>() {
			int index = 0;

			@Override
			protected JsonNode loadNext() {
				if (index < children.length)
					return children[index++];
				return noMoreElements();
			}
		};
	}

	@Override
	public void serialize(JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
	}

	@Override
	public JsonNode path(String fieldName) {
		return null;
	}

	@Override
	public JsonNode path(int index) {
		return children[index];
	}

	@Override
	public String toString() {
		return Arrays.toString(children);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(children);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CompactArrayNode other = (CompactArrayNode) obj;
		if (!Arrays.equals(children, other.children))
			return false;
		return true;
	}

}
