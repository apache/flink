package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class SopremoFunction extends Function {
	/**
	 * 
	 */
	private static final long serialVersionUID = -804125165962550321L;

	private final EvaluationExpression definition;

	public SopremoFunction(final String name, final EvaluationExpression definition) {
		super(name);
		this.definition = definition;
	}

	@Override
	public JsonNode evaluate(JsonNode target, ArrayNode params, EvaluationContext context) {
		return this.definition.evaluate(params, context);
	}
}
