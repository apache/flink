package eu.stratosphere.sopremo;

import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.JsonNodeFactory;

import eu.stratosphere.sopremo.pact.PactJsonObject;

/**
 * Provides a set of utility functions and objects to handle json data.
 * 
 * @author Arvid Heise
 */
public class JsonUtil {
	/**
	 * A general purpose {@link ObjectMapper}. No state of this mapper should be changed. If a specifically configured
	 * ObjectMapper is needed, a new instance should be created.
	 */
	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	/**
	 * A general purpose {@link JsonNodeFactory}. No state of this node factory should be changed. If a specifically
	 * configured JsonNodeFactory is needed, a new instance should be created.
	 */
	public static final JsonNodeFactory NODE_FACTORY = JsonNodeFactory.instance;

	/**
	 * A general purpose {@link JsonFactory}. No state of this factory should be changed. If a specifically
	 * configured JsonFactory is needed, a new instance should be created.
	 */
	public static final JsonFactory FACTORY = new JsonFactory();

	public static final JsonNode ZERO = new IntNode(0);

	/**
	 * Creates an efficient read-only wrapper for the given node array.
	 * 
	 * @param nodes
	 *        the nodes to wrap
	 * @return an efficient wrapper
	 */
	public static CompactArrayNode asArray(final JsonNode... nodes) {
		return new CompactArrayNode(nodes);
	}

	/**
	 * Wraps streams of objects in a {@link JsonNode}. This is the unsafe equivalent of
	 * {@link #wrapWithNode(boolean, List)}. The node may be either fully
	 * consumed after the first use or may be resettable and thus can be used to iterate several times over the
	 * elements. The first option yields less overhead and is recommended.
	 * 
	 * @param objectIterators
	 *        the streams of objects to wrap
	 * @param resettable
	 *        true if the the array node needs to be resettable
	 * @return the node wrapping the streams
	 * @see #wrapWithNode(boolean, List)
	 */
	@SuppressWarnings("unchecked")
	public static JsonNode wrapWithNode(final boolean resettable, final Iterator<?>... objectIterators) {
		final JsonNode[] streamNodes = new JsonNode[objectIterators.length];
		for (int index = 0; index < streamNodes.length; index++)
			streamNodes[index] = wrapWithNode(resettable, (Iterator<PactJsonObject>) objectIterators[index]);
		return new CompactArrayNode(streamNodes);
	}

	/**
	 * Wraps a stream of objects in a {@link JsonNode}. The node may be either fully
	 * consumed after the first use or may be resettable and thus can be used to iterate several times over the
	 * elements. The first option yields less overhead and is recommended.
	 * 
	 * @param objectIterator
	 *        the stream of objects to wrap
	 * @param resettable
	 *        true if the the array node needs to be resettable
	 * @return the node wrapping the stream
	 */
	public static StreamArrayNode wrapWithNode(final boolean resettable, final Iterator<PactJsonObject> objectIterator) {
		return StreamArrayNode.valueOf(new UnwrappingIterator(objectIterator), resettable);
	}

	/**
	 * Wraps streams of objects in a {@link JsonNode}. The node may be either fully
	 * consumed after the first use or may be resettable and thus can be used to iterate several times over the
	 * elements. The first option yields less overhead and is recommended.
	 * 
	 * @param objectIterators
	 *        the streams of objects to wrap
	 * @param resettable
	 *        true if the the array node needs to be resettable
	 * @return the node wrapping the streams
	 */
	public static JsonNode wrapWithNode(final boolean resettable,
			final List<? extends Iterator<PactJsonObject>> objectIterators) {
		final JsonNode[] streamNodes = new JsonNode[objectIterators.size()];
		for (int index = 0; index < streamNodes.length; index++)
			streamNodes[index] = wrapWithNode(resettable, objectIterators.get(index));
		return new CompactArrayNode(streamNodes);
	}
	
}
