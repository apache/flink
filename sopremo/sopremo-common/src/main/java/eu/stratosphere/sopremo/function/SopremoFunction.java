package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * A base for built-in and user-defined functions.
 * 
 * @author Arvid Heise
 */
public abstract class SopremoFunction extends Callable<IJsonNode, IArrayNode> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6012084967789433003L;
	
	protected void checkParameters(IArrayNode parameters, int expectedNumber) {
		if (expectedNumber != parameters.size())
			throw new IllegalArgumentException(String.format("Expected %s parameters", expectedNumber));
	}
}
