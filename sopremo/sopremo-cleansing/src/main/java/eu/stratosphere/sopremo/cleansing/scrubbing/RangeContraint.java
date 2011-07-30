package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.UnaryExpression;

public class RangeContraint extends BooleanExpression {
	private Class<? extends JsonNode> type;

	public RangeContraint(Class<? extends JsonNode> type) {
		this.type = type;
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return BooleanNode.valueOf(this.type.isInstance(node));
	}
}
