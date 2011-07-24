package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser.NumberType;
import org.codehaus.jackson.node.NumericNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;

@OptimizerHints(scope = Scope.NUMBER)
public class CastingExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1954495592440005318L;

	private final NumberType targetType;

	public CastingExpression(NumberType targetType) {
		this.targetType = targetType;
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		if (!(node instanceof NumericNode))
			throw new EvaluationException(String.format("The given node %s is not a number and cannot be casted", node));
		return NumberCoercer.INSTANCE.coerce((NumericNode) node, this.targetType);
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append('(').append(this.targetType).append(')');
	}
}
