package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.tree.ChildIterator;
import eu.stratosphere.sopremo.expressions.tree.NamedChildIterator;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * This traceExpression logs the evaluation of an {@link EvaluationExpression} with the help of {@link SopremoUtil.LOG}.
 */
public class TraceExpression extends EvaluationExpression implements ExpressionParent {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3935412444889830869L;

	private CachingExpression<IJsonNode> traceExpression;

	/**
	 * Initializes a TraceExpression with the given {@link EvaluationExpression}.
	 * 
	 * @param traceExpression
	 *        the traceExpression where the evauation should be logged
	 */
	public TraceExpression(final EvaluationExpression expression) {
		this.traceExpression = CachingExpression.ofAny(expression);
	}

	/**
	 * Initializes TraceExpression.
	 */
	public TraceExpression() {
		this(VALUE);
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, final IJsonNode target, final EvaluationContext context) {
		SopremoUtil.LOG.info(this.traceExpression.evaluate(node, context));
		return node;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ExpressionParent#iterator()
	 */
	@Override
	public ChildIterator iterator() {
		return new NamedChildIterator("preprocessing") {

			@Override
			protected void set(int index, EvaluationExpression e) {
				TraceExpression.this.traceExpression.innerExpression = e;
			}

			@Override
			protected EvaluationExpression get(int index) {
				return TraceExpression.this.traceExpression.innerExpression;
			}
		};
	}
}
