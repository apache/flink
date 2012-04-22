package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Represents all expressions which are not evaluable.
 */
@OptimizerHints(scope = Scope.ANY)
public class UnevaluableExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4981486971746131857L;

	private final String message;

	/**
	 * Initializes an UnevaluableExpression with the given message.
	 * 
	 * @param message
	 *        the message of this expression
	 */
	public UnevaluableExpression(final String message) {
		this.message = message;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final UnevaluableExpression other = (UnevaluableExpression) obj;
		return this.message.equals(other.message);
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		throw new EvaluationException(this.message);
	}

	@Override
	public int hashCode() {
		return 31 * super.hashCode() + this.message.hashCode();
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append(this.message);
	}

}