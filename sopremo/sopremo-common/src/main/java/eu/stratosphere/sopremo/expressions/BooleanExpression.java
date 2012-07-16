package eu.stratosphere.sopremo.expressions;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.SerializableSopremoType;

/**
 * Represents all expressions with a boolean semantic.
 */
public abstract class BooleanExpression extends EvaluationExpression implements SerializableSopremoType {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9132030265765689872L;

	public static List<BooleanExpression> ensureBooleanExpressions(List<? extends EvaluationExpression> expressions) {
		final List<BooleanExpression> booleanExpressions = new ArrayList<BooleanExpression>();
		for (int index = 0; index < expressions.size(); index++)
			if (expressions.get(index) instanceof BooleanExpression)
				booleanExpressions.add((BooleanExpression) expressions.get(index));
			else
				booleanExpressions.add(new UnaryExpression(expressions.get(index)));
		return booleanExpressions;
	}
}