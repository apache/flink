package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser.NumberType;
import org.codehaus.jackson.node.NumericNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.NumberCoercer;
import eu.stratosphere.sopremo.TypeCoercer;

@OptimizerHints(scope = Scope.NUMBER)
public class CoerceExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1954495592440005318L;

	private final Class<? extends JsonNode> targetType;

	public CoerceExpression(Class<? extends JsonNode> targetType) {
		this.targetType = targetType;
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return TypeCoercer.INSTANCE.coerce(node, this.targetType);
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append('(').append(this.targetType).append(')');
	}
}
