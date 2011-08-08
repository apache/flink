package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.TypeCoercer;

@OptimizerHints(scope = Scope.NUMBER)
public class CoerceExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1954495592440005318L;

	private final Class<? extends JsonNode> targetType;

	public CoerceExpression(final Class<? extends JsonNode> targetType) {
		this.targetType = targetType;
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		return TypeCoercer.INSTANCE.coerce(node, this.targetType);
	}

	@Override
	protected void toString(final StringBuilder builder) {
		builder.append('(').append(this.targetType).append(')');
	}
}
