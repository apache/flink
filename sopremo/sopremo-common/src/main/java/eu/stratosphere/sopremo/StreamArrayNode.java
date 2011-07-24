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
public abstract class StreamArrayNode extends ContainerNode {
	StreamArrayNode() {
		super(null);
	};

	@Override
	public JsonToken asToken() {
		return JsonToken.START_ARRAY;
	};

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
	 * /* Implementation of core JsonNode API
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
	public JsonNode get(String fieldName) {
		return null;
	}

	/*
	 * /**********************************************************
	 * /* Public API, finding value nodes
	 * /**********************************************************
	 */
	@Override
	public boolean isArray() {
		return true;
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
	public JsonNode path(String fieldName) {
		return MissingNode.getInstance();
	}

	@Override
	public ArrayNode removeAll() {
		throw new UnsupportedOperationException();
	}

	// equals and hashCode are added to avoid an EqualsVerifier bug
	@Override
	public boolean equals(Object obj) {
		return obj == this;
	}

	public abstract boolean isEmpty();

	@Override
	public int hashCode() {
		return 0;
	}

	public StreamArrayNode ensureResettable() {
		return this;
	}

	/**
	 * Creates a StreamArrayNode with the given {@link Iterator} of {@link JsonNode}s. The node may be either fully
	 * consumed after the first use or may be resettable and thus can be used to iterate several times over the
	 * elements. The first option yields less overhead and is recommended.
	 * 
	 * @param nodes
	 *        the nodes to wrap
	 * @param resettable
	 *        true if the the array node needs to be resettable
	 * @return the wrapping {@link StreamArrayNode}
	 */
	public static StreamArrayNode valueOf(Iterator<JsonNode> nodes, boolean resettable) {
		// TODO: fix with Resettable Iterator!
		if (resettable)
			return new Resettable(nodes);
		return new Iterating(nodes);
	}

	/*
	 * /**********************************************************
	 * /* Extended ObjectNode API, accessors
	 * /**********************************************************
	 */

	private static class Iterating extends StreamArrayNode implements Iterator<JsonNode> {
		private Iterator<JsonNode> nodes;

		private JsonNode currentNode;

		private int currentIndex = -1;

		public Iterating(Iterator<JsonNode> nodes) {
			this.nodes = nodes;
		}

		private void assumeBeginning() {
			if (this.currentIndex != -1)
				throw new IllegalStateException("The operations demands that the StreamArrayNode remains untouched");
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (this.getClass() != obj.getClass())
				return false;
			Resettable other = (Resettable) obj;
			return this.nodes.equals(other.nodes);
		}

		@Override
		public JsonNode get(int index) {
			if (index < this.currentIndex)
				throw new IllegalArgumentException("Cannot access previous entry");
			this.skip(index - this.currentIndex);
			return this.currentNode;
		}

		@Override
		public boolean isEmpty() {
			return !this.nodes.hasNext();
		}

		@Override
		public Iterator<JsonNode> getElements() {
			return this;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + this.nodes.hashCode();
			return result;
		}

		@Override
		public boolean hasNext() {
			return this.nodes.hasNext();
		}

		@Override
		public JsonNode next() {
			this.currentIndex++;
			return this.nodes.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public final void serialize(JsonGenerator jg, SerializerProvider provider)
				throws IOException, JsonProcessingException {
			this.assumeBeginning();
			jg.writeStartArray();
			while (this.nodes.hasNext())
				((BaseJsonNode) this.nodes.next()).writeTo(jg);
			jg.writeEndArray();
		}

		@Override
		public int size() {
			return 0;
		}

		private void skip(int count) {
			for (int index = 0; index < count; index++)
				this.currentNode = this.next();
		}

		@Override
		public boolean isResettable() {
			return false;
		}

		@Override
		public String toString() {
			return "[?]";
		}

		@Override
		public StreamArrayNode ensureResettable() {
			return StreamArrayNode.valueOf(this.nodes, true);
		}
	}

	private static class Resettable extends StreamArrayNode {
		private List<JsonNode> nodes = new ArrayList<JsonNode>();

		public Resettable(Iterator<JsonNode> nodes) {
			while (nodes.hasNext())
				this.nodes.add(nodes.next());
		}

		@Override
		public boolean isEmpty() {
			return this.nodes.isEmpty();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (this.getClass() != obj.getClass())
				return false;
			Resettable other = (Resettable) obj;
			return this.nodes.equals(other.nodes);
		}

		@Override
		public JsonNode get(int index) {
			return this.nodes.get(index);
		}

		@Override
		public Iterator<JsonNode> getElements() {
			return this.nodes.iterator();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + this.nodes.hashCode();
			return result;
		}

		@Override
		public final void serialize(JsonGenerator jg, SerializerProvider provider)
				throws IOException, JsonProcessingException {
			jg.writeStartArray();
			for (JsonNode node : this.nodes)
				((BaseJsonNode) node).writeTo(jg);
			jg.writeEndArray();
		}

		@Override
		public int size() {
			return this.nodes.size();
		}

		@Override
		public boolean isResettable() {
			return true;
		}

		@Override
		public String toString() {
			return this.nodes.toString();
		}
	}

	/**
	 * Returns true if this StreamArrayNode is resettable. If it is not resettable, it may only be iterated once over
	 * its elements.
	 * 
	 * @return true if this StreamArrayNode is resettable
	 */
	public abstract boolean isResettable();
}
