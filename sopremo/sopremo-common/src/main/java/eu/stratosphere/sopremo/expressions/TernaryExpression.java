package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.TypeCoercer;
import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
public class TernaryExpression extends EvaluationExpression {

	private EvaluationExpression ifClause, ifExpression, thenExpression;

	public TernaryExpression(EvaluationExpression ifClause, EvaluationExpression ifExpression,
			EvaluationExpression thenExpression) {
		this.ifClause = ifClause;
		this.ifExpression = ifExpression;
		this.thenExpression = thenExpression;
	}

	public TernaryExpression(EvaluationExpression ifClause, EvaluationExpression ifExpression) {
		this(ifClause, ifExpression, NULL);
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		if (TypeCoercer.INSTANCE.coerce(ifClause.evaluate(node, context), BooleanNode.class) == BooleanNode.TRUE)
			return ifExpression.evaluate(node, context);
		return thenExpression.evaluate(node, context);
	}

	@Override
	protected void toString(StringBuilder builder) {
		ifClause.toString(builder);
		builder.append(" ? ");
		ifExpression.toString(builder);
		builder.append(" : ");
		thenExpression.toString(builder);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ifClause == null) ? 0 : ifClause.hashCode());
		result = prime * result + ((ifExpression == null) ? 0 : ifExpression.hashCode());
		result = prime * result + ((thenExpression == null) ? 0 : thenExpression.hashCode());
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

		TernaryExpression other = (TernaryExpression) obj;
		if (ifClause == null) {
			if (other.ifClause != null)
				return false;
		} else if (!ifClause.equals(other.ifClause))
			return false;
		if (ifExpression == null) {
			if (other.ifExpression != null)
				return false;
		} else if (!ifExpression.equals(other.ifExpression))
			return false;
		if (thenExpression == null) {
			if (other.thenExpression != null)
				return false;
		} else if (!thenExpression.equals(other.thenExpression))
			return false;

		return true;
	}

}
