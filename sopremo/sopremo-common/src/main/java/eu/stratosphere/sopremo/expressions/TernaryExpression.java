package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.TypeCoercer;

public class TernaryExpression extends EvaluationExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7083052112301970387L;

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
		if (TypeCoercer.INSTANCE.coerce(this.ifClause.evaluate(node, context), BooleanNode.class) == BooleanNode.TRUE)
			return this.ifExpression.evaluate(node, context);
		return this.thenExpression.evaluate(node, context);
	}

	@Override
	protected void toString(StringBuilder builder) {
		this.ifClause.toString(builder);
		builder.append(" ? ");
		this.ifExpression.toString(builder);
		builder.append(" : ");
		this.thenExpression.toString(builder);
	}
}
