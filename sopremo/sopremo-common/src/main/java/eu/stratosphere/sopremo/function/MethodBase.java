package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;

/**
 * A base for built-in and user-defined functions.
 * 
 * @author Arvid Heise
 */
public abstract class MethodBase implements SerializableSopremoType {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6012084967789433003L;

	private final String name;

	/**
	 * Initializes a Function with the given name.
	 * 
	 * @param name
	 *        the name of this function
	 */
	protected MethodBase(final String name) {
		this.name = name;
	}

	/**
	 * Returns the name of this function.
	 * 
	 * @return the name
	 */
	public String getName() {
		return this.name;
	}

	@Override
	public String toString() {
		return this.name + "()";
	}

	public abstract JsonNode evaluate(JsonNode target, ArrayNode params, EvaluationContext context);
}
