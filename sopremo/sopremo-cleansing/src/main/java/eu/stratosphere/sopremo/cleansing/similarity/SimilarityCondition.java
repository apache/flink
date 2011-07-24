package eu.stratosphere.sopremo.cleansing.similarity;

import org.codehaus.jackson.JsonNode;

import uk.ac.shef.wit.simmetrics.similaritymetrics.InterfaceStringMetric;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.AndExpression;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class SimilarityCondition extends BooleanExpression {
	private EvaluationExpression metric;

	private double threshold;

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return null;
	}

}
