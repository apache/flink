package eu.stratosphere.sopremo.base;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.JsonNode;

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

	private List<EvaluationExpression> projections = new ArrayList<EvaluationExpression>();

	public ValueSplit addProjection(EvaluationExpression... projections) {
		for (EvaluationExpression evaluationExpression : projections)
			this.projections.add(evaluationExpression);
		return this;
	}

	public static class Implementation extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {
		private List<EvaluationExpression> projections = new ArrayList<EvaluationExpression>();

		@Override
		protected void map(JsonNode key, JsonNode value, JsonCollector out) {
			for (EvaluationExpression projection : this.projections)
				out.collect(key, projection.evaluate(value, this.getContext()));
		}
	}
}
