package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.TypeCoercer;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class TernaryExpression extends EvaluationExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5854293822552106472L;

	private EvaluationExpression ifClause, ifExpression, thenExpression;

	public TernaryExpression(final EvaluationExpression ifClause, final EvaluationExpression ifExpression,
			final EvaluationExpression thenExpression) {
		this.ifClause = ifClause;
		this.ifExpression = ifExpression;
		this.thenExpression = thenExpression;
	}

	public TernaryExpression(final EvaluationExpression ifClause, final EvaluationExpression ifExpression) {
		this(ifClause, ifExpression, NULL);
	}

	public EvaluationExpression getIfClause() {
		return this.ifClause;
	}

	public void setIfClause(final EvaluationExpression ifClause) {
		if (ifClause == null)
			throw new NullPointerException("ifClause must not be null");

		this.ifClause = ifClause;
	}

	public EvaluationExpression getIfExpression() {
		return this.ifExpression;
	}

	public void setIfExpression(final EvaluationExpression ifExpression) {
		if (ifExpression == null)
			throw new NullPointerException("ifExpression must not be null");

		this.ifExpression = ifExpression;
	}

	public EvaluationExpression getThenExpression() {
		return this.thenExpression;
	}

	public void setThenExpression(final EvaluationExpression thenExpression) {
		if (thenExpression == null)
			throw new NullPointerException("thenExpression must not be null");

		this.thenExpression = thenExpression;
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		if (TypeCoercer.INSTANCE.coerce(this.ifClause.evaluate(node, null, context), BooleanNode.class) == BooleanNode.TRUE)
			return this.ifExpression.evaluate(node, null, context);
		return this.thenExpression.evaluate(node, null, context);
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
