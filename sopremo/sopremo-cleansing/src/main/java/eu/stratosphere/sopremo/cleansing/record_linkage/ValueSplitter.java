package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
import eu.stratosphere.sopremo.pact.SopremoMap;

public class ValueSplitter extends ElementaryOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2967507260239105002L;

	private List<EvaluationExpression> projections = new ArrayList<EvaluationExpression>();

	public ValueSplitter(JsonStream input) {
		super(input);
	}

	public ValueSplitter addProjection(EvaluationExpression... projections) {
		for (EvaluationExpression evaluationExpression : projections)
			this.projections.add(evaluationExpression);
		return this;
	}

	public static class Implementation extends SopremoMap<Key, PactJsonObject, Key, PactJsonObject> {
		private List<EvaluationExpression> projections = new ArrayList<EvaluationExpression>();

		@Override
		protected void map(JsonNode key, JsonNode value, JsonCollector out) {
			for (EvaluationExpression projection : this.projections)
				out.collect(key, projection.evaluate(value, this.getContext()));
		}
	}
}
