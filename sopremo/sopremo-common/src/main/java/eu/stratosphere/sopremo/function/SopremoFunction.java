package eu.stratosphere.sopremo.function;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class SopremoFunction extends Function {
	/**
	 * 
	 */
	private static final long serialVersionUID = -804125165962550321L;
	private EvaluationExpression definition;

	public SopremoFunction(String name, EvaluationExpression definition) {
		super(name);
		this.definition = definition;
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return this.definition.evaluate(node, context);
	}
}
