package eu.stratosphere.sopremo;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;

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

	/**
	 * Creates an efficient read-only wrapper for the given node array.
	 * 
	 * @param nodes
	 *        the nodes to wrap
	 * @return an efficient wrapper
	 */
	public static JsonNode asArray(JsonNode... nodes) {
		return new CompactArrayNode(nodes);
	}

}
