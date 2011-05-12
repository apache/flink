package eu.stratosphere.sopremo.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.BaseJsonNode;
import org.codehaus.jackson.node.ContainerNode;
import org.codehaus.jackson.node.MissingNode;
import org.codehaus.jackson.node.ObjectNode;

public class StreamArray extends ContainerNode {
	// TODO: fix with Resettable Iterator!
	// private Iterator<JsonNode> nodes;
	private List<JsonNode> nodes = new ArrayList<JsonNode>();

	public StreamArray(Iterator<JsonNode> nodes) {
		super(null);
		// this.nodes = nodes;
		while (nodes.hasNext()) {
			this.nodes.add(nodes.next());
		}
	}

	/*
	 * /**********************************************************
	 * /* Implementation of core JsonNode API
	 * /**********************************************************
	 */

	@Override
	public JsonToken asToken() {
		return JsonToken.START_ARRAY;
	}

	@Override
	public boolean isArray() {
		return true;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public Iterator<JsonNode> getElements() {
		return nodes.iterator();
	}

	@Override
	public JsonNode get(int index) {
		return nodes.get(index);
	}

	@Override
	public JsonNode get(String fieldName) {
		return null;
	}

	@Override
	public JsonNode path(String fieldName) {
		return MissingNode.getInstance();
	}

	@Override
	public JsonNode path(int index) {
		return null;
	}

	/*
	 * /**********************************************************
	 * /* Public API, serialization
	 * /**********************************************************
	 */

	@Override
	public final void serialize(JsonGenerator jg, SerializerProvider provider)
			throws IOException, JsonProcessingException {
		jg.writeStartArray();
		for (JsonNode node : nodes)
			((BaseJsonNode) node).writeTo(jg);
		// while (nodes.hasNext())
		// ((BaseJsonNode) nodes.next()).writeTo(jg);
		jg.writeEndArray();
	}

	/*
	 * /**********************************************************
	 * /* Public API, finding value nodes
	 * /**********************************************************
	 */

	@Override
	public JsonNode findValue(String fieldName) {
		return null;
	}

	@Override
	public List<JsonNode> findValues(String fieldName, List<JsonNode> foundSoFar) {
		return foundSoFar;
	}

	@Override
	public List<String> findValuesAsText(String fieldName, List<String> foundSoFar) {
		return foundSoFar;
	}

	@Override
	public ObjectNode findParent(String fieldName) {
		return null;
	}

	@Override
	public List<JsonNode> findParents(String fieldName, List<JsonNode> foundSoFar) {
		return foundSoFar;
	}

	/*
	 * /**********************************************************
	 * /* Extended ObjectNode API, accessors
	 * /**********************************************************
	 */

	@Override
	public ArrayNode removeAll() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return "[?]";
	}

	@Override
	public boolean equals(Object o) {
		return false;
	}
}
