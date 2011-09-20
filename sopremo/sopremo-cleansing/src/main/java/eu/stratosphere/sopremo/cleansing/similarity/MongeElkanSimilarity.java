package eu.stratosphere.sopremo.cleansing.similarity;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.DoubleNode;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class MongeElkanSimilarity extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3635602165354195576L;

	private EvaluationExpression baseMeasure;

	private final EvaluationExpression leftExpression, rightExpression;

	public MongeElkanSimilarity(EvaluationExpression baseMeasure, EvaluationExpression leftExpression,
			EvaluationExpression rightExpression) {
		this.baseMeasure = baseMeasure;
		this.leftExpression = leftExpression;
		this.rightExpression = rightExpression;
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		JsonNode leftValues = this.leftExpression.evaluate(node, context);
		JsonNode rightValues = this.rightExpression.evaluate(node, context);

		if (leftValues.size() == 0 || rightValues.size() == 0)
			return DoubleNode.valueOf(0);

		double sum = 0;
		for (JsonNode leftValue : leftValues) {
			double max = 0;
			for (JsonNode rightValue : rightValues)
				max = Math.max(max, this.baseMeasure.evaluate(JsonUtil.asArray(leftValue, rightValue), context)
					.getDoubleValue());
			sum += max;
		}

		return DoubleNode.valueOf(sum / leftValues.size());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.baseMeasure.hashCode();
		result = prime * result + this.leftExpression.hashCode();
		result = prime * result + this.rightExpression.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		MongeElkanSimilarity other = (MongeElkanSimilarity) obj;
		return this.baseMeasure.equals(other.baseMeasure) && this.leftExpression.equals(other.leftExpression)
			&& this.rightExpression.equals(other.rightExpression);
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append("monge elkan of").append(this.baseMeasure);
	}
}
