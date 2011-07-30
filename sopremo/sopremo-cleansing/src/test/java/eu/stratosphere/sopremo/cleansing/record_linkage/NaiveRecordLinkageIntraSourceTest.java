package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Collection;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.cleansing.record_linkage.Naive;
import eu.stratosphere.sopremo.cleansing.record_linkage.RecordLinkage;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

/**
 * Tests {@link Naive} {@link RecordLinkage} with one data source.
 * 
 * @author Arvid Heise
 */
public class NaiveRecordLinkageIntraSourceTest extends IntraSourceRecordLinkageTestBase<Naive> {
	private double threshold;

	private boolean useId;

	private EvaluationExpression projection;

	/**
	 * Initializes NaiveRecordLinkageIntraSourceTest with the given parameter
	 * 
	 * @param threshold
	 * @param useId
	 * @param projection
	 */
	public NaiveRecordLinkageIntraSourceTest(double threshold, boolean useId, EvaluationExpression projection) {
		this.threshold = threshold;
		this.useId = useId;
		this.projection = projection == null ? EvaluationExpression.SAME_VALUE : projection;
	}

	/**
	 * Returns the parameter combination under test.
	 * 
	 * @return the parameter combination
	 */
	@Parameters
	public static Collection<Object[]> getParameters() {
		EvaluationExpression[] projections = { null, getAggregativeProjection() };
		double[] thresholds = { 0.0, 0.2, 0.4, 0.6, 0.8, 1.0 };
		boolean[] useIds = { false, true };

		ArrayList<Object[]> parameters = new ArrayList<Object[]>();
		for (EvaluationExpression projection : projections)
			for (double threshold : thresholds)
				for (boolean useId : useIds)
					parameters.add(new Object[] { threshold, useId, projection });

		return parameters;
	}

	/**
	 * Performs the naive record linkage in place and compares with the Pact code.
	 */
	@Test
	public void pactCodeShouldPerformLikeStandardImplementation() {
		EvaluationExpression similarityFunction = this.getSimilarityFunction();
		RecordLinkage recordLinkage = new RecordLinkage(new Naive(), similarityFunction, this.threshold, (JsonStream) null);
		SopremoTestPlan sopremoTestPlan = this.createTestPlan(recordLinkage, this.useId, this.projection);

		EvaluationExpression duplicateProjection = this.projection;
		if (duplicateProjection == null)
			duplicateProjection = new ArrayCreation(new PathExpression(new InputSelection(0),
				recordLinkage.getIdProjection(0)), new PathExpression(new InputSelection(1),
				recordLinkage.getIdProjection(1)));

		EvaluationContext context = sopremoTestPlan.getEvaluationContext();
		for (KeyValuePair<Key, PactJsonObject> left : sopremoTestPlan.getInput(0)) {
			boolean skipPairs = true;
			for (KeyValuePair<Key, PactJsonObject> right : sopremoTestPlan.getInput(0)) {
				if (left == right) {
					skipPairs = false;
					continue;
				} else if (skipPairs)
					continue;

				JsonNode pair = createOrderedPair(left.getValue().getValue(), right.getValue().getValue());
				if (similarityFunction.evaluate(pair, context).getDoubleValue() > this.threshold)
					sopremoTestPlan.getExpectedOutput(0).add(
						new PactJsonObject(duplicateProjection.evaluate(pair, context)));
			}
		}
		sopremoTestPlan.run();
	}
}
