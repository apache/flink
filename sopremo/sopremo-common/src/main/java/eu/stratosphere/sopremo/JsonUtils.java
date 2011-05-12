package eu.stratosphere.sopremo;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;

public class JsonUtils {

	public static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static JsonNodeFactory NODE_FACTORY = JsonNodeFactory.instance;

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
