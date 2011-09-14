package eu.stratosphere.sopremo.cleansing.similarity;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class SimilarityCondition extends BooleanExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2016585535661456291L;

	private EvaluationExpression metric;

	private double threshold;

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		return null;
	}

}
