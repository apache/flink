package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.sopremo.CompactArrayNode;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

/**
 * Tests {@link Naive} {@link RecordLinkage} with two data sources.
 * 
 * @author Arvid Heise
 */
public class NaiveRecordLinkageInterSourceTest extends InterSourceRecordLinkageTestBase<Naive> {

	private double threshold;

	private EvaluationExpression projection;

	/**
	 * Initializes NaiveRecordLinkageInterSourceTest with the given parameter
	 * 
	 * @param threshold
	 * @param projection
	 */
	public NaiveRecordLinkageInterSourceTest(double threshold, EvaluationExpression projection) {
		this.threshold = threshold;
		this.projection = projection;
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

		ArrayList<Object[]> parameters = new ArrayList<Object[]>();
		for (EvaluationExpression projection : projections)
			for (double threshold : thresholds)
				parameters.add(new Object[] { threshold, projection });

		return parameters;
	}

	/**
	 * Performs the naive record linkage in place and compares with the Pact code.
	 */
	@Test
	public void pactCodeShouldPerformLikeStandardImplementation() {
		EvaluationExpression similarityFunction = getSimilarityFunction();
		RecordLinkage recordLinkage = new RecordLinkage(new Naive(), similarityFunction, this.threshold, null, null);
		SopremoTestPlan sopremoTestPlan = createTestPlan(recordLinkage, false, this.projection);

		EvaluationExpression duplicateProjection = this.projection;
		if (duplicateProjection == null)
			duplicateProjection = new ArrayCreation(new PathExpression(new InputSelection(0),
				recordLinkage.getIdProjection(0)), new PathExpression(new InputSelection(1),
				recordLinkage.getIdProjection(1)));

		EvaluationContext context = sopremoTestPlan.getEvaluationContext();
		for (KeyValuePair<Key, PactJsonObject> left : sopremoTestPlan.getInput(0))
			for (KeyValuePair<Key, PactJsonObject> right : sopremoTestPlan.getInput(1)) {
				CompactArrayNode pair = JsonUtil.asArray(left.getValue().getValue(), right.getValue().getValue());
				if (similarityFunction.evaluate(pair, context).getDoubleValue() > this.threshold)
					sopremoTestPlan.getExpectedOutput(0).add(
						new PactJsonObject(duplicateProjection.evaluate(pair, context)));
			}
		sopremoTestPlan.run();
	}
}
