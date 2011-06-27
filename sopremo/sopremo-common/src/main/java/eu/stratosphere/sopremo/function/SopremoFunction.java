package eu.stratosphere.sopremo.function;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;

public class SopremoFunction extends Function {
	/**
	 * 
	 */
	private static final long serialVersionUID = -804125165962550321L;
	private EvaluableExpression definition;

	public SopremoFunction(String name, EvaluableExpression definition) {
		super(name);
		this.definition = definition;
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return this.definition.evaluate(node, context);
	}
}
