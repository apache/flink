package eu.stratosphere.sopremo.expressions;

import org.eclipse.jetty.util.ajax.JSONPojoConvertor.NumberType;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.NumberCoercer;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NumericNode;

@OptimizerHints(scope = Scope.NUMBER)
public class NumberCastingExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1954495592440005318L;

	private final NumberType targetType;

	public NumberCastingExpression(final NumberType targetType) {
		this.targetType = targetType;
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		if (!(node instanceof NumericNode))
			throw new EvaluationException(String.format("The given node %s is not a number and cannot be casted", node));
		return NumberCoercer.INSTANCE.coerce((NumericNode) node, this.targetType);
	}

	@Override
	protected void toString(final StringBuilder builder) {
		builder.append('(').append(this.targetType).append(')');
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((targetType == null) ? 0 : targetType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;

		NumberCastingExpression other = (NumberCastingExpression) obj;
		if (targetType != other.targetType)
			return false;

		return true;
	}

}
