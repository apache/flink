package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * This expression logs the evaluation of an {@link EvaluationExpression} with the help of {@link SopremoUtil.LOG}.
 */
public class TraceExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3935412444889830869L;

	private final EvaluationExpression expression;

	/**
	 * Initializes a TraceExpression with the given {@link EvaluationExpression}.
	 * 
	 * @param expression
	 *        the expression where the evauation should be logged
	 */
	public TraceExpression(final EvaluationExpression expression) {
		this.expression = expression;
	}

	/**
	 * Initializes TraceExpression.
	 */
	public TraceExpression() {
		this(VALUE);
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		SopremoUtil.LOG.info(this.expression.evaluate(node, target, context));
		return node;
	}

}
