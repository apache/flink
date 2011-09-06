package eu.stratosphere.usecase.cleansing;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class TraceExpression extends EvaluationExpression {
private EvaluationExpression expression;


	public TraceExpression(EvaluationExpression expression) {
	this.expression = expression;
}

	public TraceExpression() {
		this(VALUE);
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		SopremoUtil.LOG.info(expression.evaluate(node, context));
		return node;
	}

}
