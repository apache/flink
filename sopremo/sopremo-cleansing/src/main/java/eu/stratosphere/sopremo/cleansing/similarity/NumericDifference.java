package eu.stratosphere.sopremo.cleansing.similarity;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.NumericNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class NumericDifference extends EvaluationExpression {
	private double maxDiff;

	private EvaluationExpression leftExpression, rightExpression;

	public NumericDifference(EvaluationExpression leftExpression, EvaluationExpression rightExpression, double maxDiff) {
		this.leftExpression = leftExpression;
		this.rightExpression = rightExpression;
		this.maxDiff = maxDiff;
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		double left = leftExpression.evaluate(node, context).getDoubleValue();
		double right = rightExpression.evaluate(node, context).getDoubleValue();
		double diff = Math.abs(left - right);
		if (diff > maxDiff)
			return JsonUtil.ZERO;
		return DoubleNode.valueOf(1 - diff / maxDiff);
	}
}
