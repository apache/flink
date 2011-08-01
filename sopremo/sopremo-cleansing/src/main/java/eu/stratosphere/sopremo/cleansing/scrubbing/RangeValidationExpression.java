package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.UnaryExpression;

public class RangeValidationExpression extends ValidationExpression {
	private JsonNode min, max;

	public RangeValidationExpression(EvaluationExpression source, JsonNode min, JsonNode max) {
		super(source);
		this.min = min;
		this.max = max;
	}

	@Override
	protected boolean validate(JsonNode sourceNode, JsonNode possibleResult, EvaluationContext context) {
		return false;
	}
}
