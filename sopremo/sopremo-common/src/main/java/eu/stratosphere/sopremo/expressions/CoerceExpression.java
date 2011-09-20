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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.targetType == null ? 0 : this.targetType.hashCode());
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

		CoerceExpression other = (CoerceExpression) obj;
		if (this.targetType == null) {
			if (other.targetType != null)
				return false;
		} else if (!this.targetType.equals(other.targetType))
			return false;

		return true;
	}

}
