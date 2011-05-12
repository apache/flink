package eu.stratosphere.sopremo;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.expressions.EvaluableExpression;

public class SopremoFunction extends Function {
	private EvaluableExpression definition;

	public SopremoFunction(String name, EvaluableExpression definition) {
		super(name);
		this.definition = definition;
	}

	@Override
	public JsonNode evaluate(JsonNode node) {
		return definition.evaluate(node);
	}
}
