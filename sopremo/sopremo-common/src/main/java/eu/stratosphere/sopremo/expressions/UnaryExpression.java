package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.TypeCoercer;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;

@OptimizerHints(scope = Scope.ANY)
public class UnaryExpression extends BooleanExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4256326224698296602L;

	private final EvaluationExpression expr;

	private boolean negate = false;

	public UnaryExpression(final EvaluationExpression booleanExpr) {
		this(booleanExpr, false);
	}

	public UnaryExpression(final EvaluationExpression expr, final boolean negate) {
		this.expr = expr;
		this.negate = negate;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final UnaryExpression other = (UnaryExpression) obj;
		return this.expr.equals(other.expr) && this.negate == other.negate;
	}

	public static BooleanExpression wrap(final EvaluationExpression expression) {
		if (expression instanceof BooleanExpression)
			return (BooleanExpression) expression;
		return new UnaryExpression(expression);
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		final BooleanNode result = TypeCoercer.INSTANCE.coerce(this.expr.evaluate(node, target, context), BooleanNode.class);
		if (this.negate)
			return result == BooleanNode.TRUE ? BooleanNode.FALSE : BooleanNode.TRUE;
		return result;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.expr.hashCode();
		result = prime * result + (this.negate ? 1231 : 1237);
		return result;
	}

	@Override
	public void toString(final StringBuilder builder) {
		if (this.negate)
			builder.append("!");
		builder.append(this.expr);
	}

}