package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.SerializableSopremoType;

/**
 * Represents all expressions with a boolean semantic.
 */
public abstract class BooleanExpression extends EvaluationExpression implements SerializableSopremoType {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9132030265765689872L;
}