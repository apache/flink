package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;

/**
 * A base for built-in and user-defined functions.
 * 
 * @author Arvid Heise
 */
public abstract class JsonMethod extends Callable<JsonNode, ArrayNode> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6012084967789433003L;

	/**
	 * Initializes a Function with the given name.
	 * 
	 * @param name
	 *        the name of this function
	 */
	protected JsonMethod(final String name) {
		super(name);
	}
}
