package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.pact.JsonNodeComparator;

public class RangeValidationExpression extends ValidationRule {
	private JsonNode min, max;

	public RangeValidationExpression(JsonNode min, JsonNode max, ObjectAccess... targetPath) {
		super(targetPath);
		this.min = min;
		this.max = max;
	}

	@Override
	protected JsonNode fix(JsonNode possibleResult, JsonNode sourceNode, EvaluationContext context) {
		if (JsonNodeComparator.INSTANCE.compare(min, possibleResult) > 0) {
			if (getValueCorrection() != DEFAULT_CORRECTION)
				return super.fix(possibleResult, sourceNode, context);
			return min;
		}
		if (JsonNodeComparator.INSTANCE.compare(possibleResult, max) > 0) {
			if (getValueCorrection() != DEFAULT_CORRECTION)
				return super.fix(possibleResult, sourceNode, context);
			return max;
		}
		return possibleResult;
	}
}
