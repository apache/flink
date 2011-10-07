package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.TypeCoercer;
import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class TernaryExpression extends EvaluationExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5854293822552106472L;

	private final EvaluationExpression ifClause, ifExpression, thenExpression;

	public TernaryExpression(final EvaluationExpression ifClause, final EvaluationExpression ifExpression,
			final EvaluationExpression thenExpression) {
		this.ifClause = ifClause;
		this.ifExpression = ifExpression;
		this.thenExpression = thenExpression;
	}

	public TernaryExpression(final EvaluationExpression ifClause, final EvaluationExpression ifExpression) {
		this(ifClause, ifExpression, NULL);
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		if (TypeCoercer.INSTANCE.coerce(this.ifClause.evaluate(node, context), BooleanNode.class) == BooleanNode.TRUE)
			return this.ifExpression.evaluate(node, context);
		return this.thenExpression.evaluate(node, context);
	}

	@Override
	protected void toString(final StringBuilder builder) {
		this.ifClause.toString(builder);
		builder.append(" ? ");
		this.ifExpression.toString(builder);
		builder.append(" : ");
		this.thenExpression.toString(builder);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.ifClause == null ? 0 : this.ifClause.hashCode());
		result = prime * result + (this.ifExpression == null ? 0 : this.ifExpression.hashCode());
		result = prime * result + (this.thenExpression == null ? 0 : this.thenExpression.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;

		final TernaryExpression other = (TernaryExpression) obj;
		if (this.ifClause == null) {
			if (other.ifClause != null)
				return false;
		} else if (!this.ifClause.equals(other.ifClause))
			return false;
		if (this.ifExpression == null) {
			if (other.ifExpression != null)
				return false;
		} else if (!this.ifExpression.equals(other.ifExpression))
			return false;
		if (this.thenExpression == null) {
			if (other.thenExpression != null)
				return false;
		} else if (!this.thenExpression.equals(other.thenExpression))
			return false;

		return true;
	}

}
