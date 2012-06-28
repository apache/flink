package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.expressions.CachingExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCross;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Implements a projection which gets contextual information through a logical side channel.<br>
 * The side channel is given by the second input source.<br>
 * This operator is useful, when a projection needs the result of a prior global aggregation.
 * 
 * @author Arvid Heise
 */
@InputCardinality(2)
public class ContextualProjection extends ElementaryOperator<ContextualProjection> {
	private EvaluationExpression contextPath = new ObjectAccess("context");

	/**
	 * 
	 */
	private static final long serialVersionUID = -3170859806301947764L;

	public EvaluationExpression getContextPath() {
		return this.contextPath;
	}

	public void setContextPath(final EvaluationExpression contextPath) {
		if (contextPath == null)
			throw new NullPointerException("contextPath must not be null");

		this.contextPath = contextPath;
	}

	public ContextualProjection withContextPath(final EvaluationExpression contextPath) {
		this.setContextPath(contextPath);
		return this;
	}

	public static class Implementation extends SopremoCross {
		private CachingExpression<IJsonNode> contextPath;

		@Override
		protected void cross(final IJsonNode value, final IJsonNode context, final JsonCollector out) {
			this.contextPath.set(value, context, this.getContext());
			out.collect(value);
		}
	}
}
