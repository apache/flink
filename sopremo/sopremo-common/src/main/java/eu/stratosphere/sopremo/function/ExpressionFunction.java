package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class ExpressionFunction extends SopremoFunction implements Inlineable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -804125165962550321L;

	private final EvaluationExpression definition;

	private final int numParams;

	public ExpressionFunction(final int numParams, final EvaluationExpression definition) {
		this.definition = definition;
		this.numParams = numParams;
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
	public IJsonNode call(final IArrayNode params, final IJsonNode target, final EvaluationContext context) {
		checkParameters(params, this.numParams);
		return this.definition.evaluate(params, target, context);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		builder.append("Sopremo function ");
		this.definition.toString(builder);
	}
}
