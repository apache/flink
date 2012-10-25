package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.tree.ChildIterator;
import eu.stratosphere.sopremo.expressions.tree.NamedChildIterator;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TypeCoercer;

/**
 * Represents a unary boolean expression.
 */
@OptimizerHints(scope = Scope.ANY)
public class UnaryExpression extends BooleanExpression implements ExpressionParent {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4256326224698296602L;

	private EvaluationExpression expr;

	private boolean negate = false;

	/**
	 * Initializes an UnaryExpression with the given {@link EvaluationExpression}.
	 * 
	 * @param booleanExpr
	 *        the expression which evaluates to the boolean value that should be represented by this
	 *        {@link UnaryExpression}
	 */
	public UnaryExpression(final EvaluationExpression booleanExpr) {
		this(booleanExpr, false);
	}

	/**
	 * Initializes an UnaryExpression with the given {@link EvaluationExpression} and the given negate-flag.
	 * 
	 * @param booleanExpr
	 *        the expression which evaluates to the boolean value that should be represented by this
	 *        {@link UnaryExpression}
	 * @param negate
	 *        indicates either the result of the evaluation should be negated or not
	 */
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

	@Override
	public IJsonNode evaluate(final IJsonNode node, final IJsonNode target, final EvaluationContext context) {
		// no need to reuse target of coercion - no new boolean node is created anew
		final BooleanNode result =
			TypeCoercer.INSTANCE.coerce(this.expr.evaluate(node, target, context), null, BooleanNode.class);

		// we can ignore 'target' because no new Object is created
		if (this.negate)
			return result == BooleanNode.TRUE ? BooleanNode.FALSE : BooleanNode.TRUE;
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ExpressionParent#iterator()
	 */
	@Override
	public ChildIterator iterator() {
		return new NamedChildIterator("expr") {

			@Override
			protected void set(int index, EvaluationExpression e) {
				UnaryExpression.this.expr = e;
			}

			@Override
			protected EvaluationExpression get(int index) {
				return UnaryExpression.this.expr;
			}
		};
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