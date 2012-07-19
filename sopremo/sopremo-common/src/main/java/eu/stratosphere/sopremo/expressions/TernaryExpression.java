package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TypeCoercer;

/**
 * Represents a if-then-else clause.
 */
public class TernaryExpression extends EvaluationExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5854293822552106472L;

	private CachingExpression<IJsonNode> ifClause;

	private EvaluationExpression ifExpression, thenExpression;

	/**
	 * Initializes a TernaryExpression with the given {@link EvaluationExpression}s.
	 * 
	 * @param ifClause
	 *        the expression that represents the condition of this {@link TernaryExpression}
	 * @param ifExpression
	 *        the expression that should be evaluated if the iFClause evaluation results in {@link BooleanNode.TRUE}
	 * @param thenExpression
	 *        the expression that should be evaluated if the iFClause evaluation results in {@link BooleanNode.FALSE}
	 */
	public TernaryExpression(final EvaluationExpression ifClause, final EvaluationExpression ifExpression,
			final EvaluationExpression thenExpression) {
		this.ifClause = CachingExpression.ofSubclass(ifClause, IJsonNode.class);
		this.ifExpression = ifExpression;
		this.thenExpression = thenExpression;
	}

	/**
	 * Initializes a TernaryExpression with the given {@link EvaluationExpression}s.
	 * 
	 * @param ifClause
	 *        the expression that represents the condition of this {@link TernaryExpression}
	 * @param ifExpression
	 *        the expression that should be evaluated if the iFClause evaluation results in {@link BooleanNode.TRUE}
	 */
	public TernaryExpression(final EvaluationExpression ifClause, final EvaluationExpression ifExpression) {
		this(ifClause, ifExpression, ConstantExpression.MISSING);
	}

	/**
	 * Returns the ifClause-expression
	 * 
	 * @return the ifClause-expression
	 */
	public EvaluationExpression getIfClause() {
		return this.ifClause;
	}

	/**
	 * Sets a new ifClausse-expression
	 * 
	 * @param ifClause
	 *        the expression that should be set as the new ifClause
	 */
	public void setIfClause(final EvaluationExpression ifClause) {
		if (ifClause == null)
			throw new NullPointerException("ifClause must not be null");

		this.ifClause = CachingExpression.ofSubclass(ifClause, IJsonNode.class);
	}

	/**
	 * Returns the ifExpression
	 * 
	 * @return the ifExpression
	 */
	public EvaluationExpression getIfExpression() {
		return this.ifExpression;
	}

	/**
	 * Sets a new ifExpression
	 * 
	 * @param ifExpression
	 *        the expression that should be set as the new ifExpression
	 */
	public void setIfExpression(final EvaluationExpression ifExpression) {
		if (ifExpression == null)
			throw new NullPointerException("ifExpression must not be null");

		this.ifExpression = ifExpression;
	}

	/**
	 * Returns the thenExpression
	 * 
	 * @return the thenExpression
	 */
	public EvaluationExpression getThenExpression() {
		return this.thenExpression;
	}

	/**
	 * Sets a new thenExpression
	 * 
	 * @param thenExpression
	 *        the expression that should be set as the new thenExpression
	 */
	public void setThenExpression(final EvaluationExpression thenExpression) {
		if (thenExpression == null)
			throw new NullPointerException("thenExpression must not be null");

		this.thenExpression = thenExpression;
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		// no need to reuse the target of the coercion - a boolean node is never created anew 
		if (TypeCoercer.INSTANCE.coerce(this.ifClause.evaluate(node, context), null, BooleanNode.class) == BooleanNode.TRUE)
			return this.ifExpression.evaluate(node, target, context);
		return this.thenExpression.evaluate(node, target, context);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.expressions.EvaluationExpression#transformRecursively(eu.stratosphere.sopremo.expressions
	 * .TransformFunction)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public EvaluationExpression transformRecursively(TransformFunction function) {
		this.ifClause = (CachingExpression<IJsonNode>) function.call(this.ifClause);
		this.ifExpression = function.call(this.ifExpression);
		this.thenExpression = function.call(this.thenExpression);
		return null;
	}

	@Override
	public void toString(final StringBuilder builder) {
		this.ifClause.toString(builder);
		builder.append(" ? ");
		this.ifExpression.toString(builder);
		builder.append(" : ");
		this.thenExpression.toString(builder);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.ifClause.hashCode();
		result = prime * result + this.ifExpression.hashCode();
		result = prime * result + this.thenExpression.hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final TernaryExpression other = (TernaryExpression) obj;
		return this.ifClause.equals(other.ifClause)
			&& this.ifExpression.equals(other.ifExpression)
			&& this.thenExpression.equals(other.thenExpression);
	}

}
