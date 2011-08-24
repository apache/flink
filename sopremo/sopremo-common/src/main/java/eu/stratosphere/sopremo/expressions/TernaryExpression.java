package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.TypeCoercer;

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
}
