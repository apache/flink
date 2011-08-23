package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;

@OptimizerHints(scope = Scope.ANY)
public class ErroneousExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4981486971746131857L;

	private final String message;

	public ErroneousExpression(final String message) {
		this.message = message;
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.message.equals(((ErroneousExpression) obj).message);
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		throw new EvaluationException(this.message);
	}

	@Override
	public int hashCode() {
		return 31 + this.message.hashCode();
	}

	@Override
	protected void toString(final StringBuilder builder) {
		builder.append("Error (").append(this.message).append(")");
	}

}