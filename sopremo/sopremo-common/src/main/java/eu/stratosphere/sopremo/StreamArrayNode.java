package eu.stratosphere.sopremo;

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

/**
 * Provides a read-only view of an iterator of {@link JsonNode}s as an json array of undefined size.<br>
 * Due to current limitations in the PACT layer, the elements are cached to allow multiple traversal.
 * 
 * @author Arvid Heise
 */
public class StreamArrayNode extends ContainerNode {
	// TODO: fix with Resettable Iterator!
	// private Iterator<JsonNode> nodes;
	private List<JsonNode> nodes = new ArrayList<JsonNode>();

	/**
	 * Initializes StreamArrayNode with the given {@link Iterator} of {@link JsonNode}s.
	 * 
	 * @param nodes
	 *        the nodes to wrap
	 */
	public StreamArrayNode(Iterator<JsonNode> nodes) {
		super(null);
		// this.nodes = nodes;
		while (nodes.hasNext())
			this.nodes.add(nodes.next());
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
	public boolean equals(Object o) {
		return o == this;
	}

	@Override
	public ObjectNode findParent(String fieldName) {
		return null;
	}

	@Override
	public List<JsonNode> findParents(String fieldName, List<JsonNode> foundSoFar) {
		return foundSoFar;
	}

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
	public JsonNode get(int index) {
		return this.nodes.get(index);
	}

	/*
	 * /**********************************************************
	 * /* Public API, serialization
	 * /**********************************************************
	 */

	@Override
	public JsonNode get(String fieldName) {
		return null;
	}

	/*
	 * /**********************************************************
	 * /* Public API, finding value nodes
	 * /**********************************************************
	 */

	@Override
	public Iterator<JsonNode> getElements() {
		return this.nodes.iterator();
	}

	@Override
	public int hashCode() {
		return System.identityHashCode(this);
	}

	@Override
	public boolean isArray() {
		return true;
	}

	@Override
	public JsonNode path(int index) {
		return null;
	}

	@Override
	public JsonNode path(String fieldName) {
		return MissingNode.getInstance();
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
	public final void serialize(JsonGenerator jg, SerializerProvider provider)
			throws IOException, JsonProcessingException {
		jg.writeStartArray();
		for (JsonNode node : this.nodes)
			((BaseJsonNode) node).writeTo(jg);
		// while (nodes.hasNext())
		// ((BaseJsonNode) nodes.next()).writeTo(jg);
		jg.writeEndArray();
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public String toString() {
		return "[?]";
	}
}
