package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.SerializableSopremoType;

/**
 * A base for built-in and user-defined functions.
 * 
 * @author Arvid Heise
 */
public abstract class Function implements Evaluable, SerializableSopremoType {
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
	protected Function(String name) {
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
}
