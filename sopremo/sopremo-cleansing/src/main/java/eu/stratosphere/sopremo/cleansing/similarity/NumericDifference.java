package eu.stratosphere.sopremo.cleansing.similarity;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.DoubleNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class NumericDifference extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7081441805385994782L;

	private final double maxDiff;

	private final EvaluationExpression leftExpression, rightExpression;

	public NumericDifference(final EvaluationExpression leftExpression, final EvaluationExpression rightExpression,
			final double maxDiff) {
		this.leftExpression = leftExpression;
		this.rightExpression = rightExpression;
		this.maxDiff = maxDiff;
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		final double left = this.leftExpression.evaluate(node, context).getDoubleValue();
		final double right = this.rightExpression.evaluate(node, context).getDoubleValue();
		final double diff = Math.abs(left - right);
		if (diff > this.maxDiff)
			return JsonUtil.ZERO;
		return DoubleNode.valueOf(1 - diff / this.maxDiff);
	}
}
