package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.JsonNode;

public abstract class CleansingRule<ContextType extends EvaluationContext> extends EvaluationExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1801909303463739160L;

	@SuppressWarnings("unchecked")
	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return evaluateRule(node, (ContextType) context);
	}

	public abstract JsonNode evaluateRule(JsonNode node, ContextType context);

	@Override
	public void toString(StringBuilder builder) {
		builder.append(getClass().getSimpleName());
	}
}