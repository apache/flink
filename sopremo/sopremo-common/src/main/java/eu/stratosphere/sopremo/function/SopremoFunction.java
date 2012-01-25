package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;

public class SopremoFunction extends JsonMethod implements Inlineable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -804125165962550321L;

	private final EvaluationExpression definition;

	public SopremoFunction(final String name, final EvaluationExpression definition) {
		super(name);
		this.definition = definition;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.function.Inlineable#getDefinition(eu.stratosphere.sopremo.expressions.EvaluationExpression
	 * [])
	 */
	@Override
	public EvaluationExpression getDefinition() {
		return this.definition;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.function.Callable#call(InputType[], eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public JsonNode call(final ArrayNode params, final EvaluationContext context) {
		return this.definition.evaluate(params, context);
	}
}
