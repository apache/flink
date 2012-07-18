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

	/**
	 * Wraps the given {@link EvaluationExpression} as a {@link BooleanExpression}.
	 * 
	 * @param expression
	 *        the expression that should be wrapped
	 * @return the wrapped expression
	 */
	public static BooleanExpression ensureBooleanExpression(final EvaluationExpression expression) {
		if (expression instanceof BooleanExpression)
			return (BooleanExpression) expression;
		return new UnaryExpression(expression);
	}

	/**
	 * Wraps the given list of {@link EvaluationExpression} as a list of {@link BooleanExpression}.
	 * 
	 * @param expression
	 *        the expression that should be wrapped
	 * @return the wrapped expression
	 */
	public static List<BooleanExpression> ensureBooleanExpressions(
			final List<? extends EvaluationExpression> expressions) {
		final ArrayList<BooleanExpression> booleans = new ArrayList<BooleanExpression>(expressions.size());
		for (final EvaluationExpression evaluationExpression : expressions)
			booleans.add(ensureBooleanExpression(evaluationExpression));
		return booleans;
	}
}