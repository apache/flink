package eu.stratosphere.sopremo.base;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.expressions.CachingExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Splits a tuple explicitly into multiple outgoing tuples.<br>
 * This operator provides a means to emit more than one tuple in contrast to most other base operators.
 * 
 * @author Arvid Heise
 */
public class ValueSplit extends ElementaryOperator<ValueSplit> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2967507260239105002L;

	private List<CachingExpression<IJsonNode>> projections = new ArrayList<CachingExpression<IJsonNode>>();

	public ValueSplit addProjection(EvaluationExpression... projections) {
		for (EvaluationExpression evaluationExpression : projections)
			this.projections.add(CachingExpression.of(evaluationExpression, IJsonNode.class));
		return this;
	}

	public static class Implementation extends SopremoMap {
		private List<CachingExpression<IJsonNode>> projections = new ArrayList<CachingExpression<IJsonNode>>();

		@Override
		protected void map(IJsonNode value, JsonCollector out) {
			for (CachingExpression<IJsonNode> projection : this.projections)
				out.collect(projection.evaluate(value, this.getContext()));
		}
	}
}
