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

	private final double threshold;

	private final EvaluationExpression projection;

	/**
	 * Initializes NaiveRecordLinkageInterSourceTest with the given parameter
	 * 
	 * @param threshold
	 * @param projection
	 */
	public NaiveRecordLinkageInterSourceTest(final double threshold, final EvaluationExpression projection) {
		this.threshold = threshold;
		this.projection = projection;
	}

	/**
	 * Performs the naive record linkage in place and compares with the Pact code.
	 */
	@Test
	public void pactCodeShouldPerformLikeStandardImplementation() {
		final EvaluationExpression similarityFunction = getSimilarityFunction();
		final RecordLinkage recordLinkage = new RecordLinkage(new Naive(), similarityFunction, this.threshold, null,
			null);
		final SopremoTestPlan sopremoTestPlan = createTestPlan(recordLinkage, false, this.projection);

		EvaluationExpression duplicateProjection = this.projection;
		if (duplicateProjection == null)
			duplicateProjection = new ArrayCreation(new PathExpression(new InputSelection(0),
				recordLinkage.getIdProjection(0)), new PathExpression(new InputSelection(1),
				recordLinkage.getIdProjection(1)));

		final EvaluationContext context = sopremoTestPlan.getEvaluationContext();
		for (final KeyValuePair<Key, PactJsonObject> left : sopremoTestPlan.getInput(0))
			for (final KeyValuePair<Key, PactJsonObject> right : sopremoTestPlan.getInput(1)) {
				final CompactArrayNode pair = JsonUtil.asArray(left.getValue().getValue(), right.getValue().getValue());
				if (similarityFunction.evaluate(pair, context).getDoubleValue() > this.threshold)
					sopremoTestPlan.getExpectedOutput(0).add(
						new PactJsonObject(duplicateProjection.evaluate(pair, context)));
			}
		sopremoTestPlan.run();
	}

	/**
	 * Returns the parameter combination under test.
	 * 
	 * @return the parameter combination
	 */
	@Parameters
	public static Collection<Object[]> getParameters() {
		final EvaluationExpression[] projections = { null, getAggregativeProjection() };
		final double[] thresholds = { 0.0, 0.2, 0.4, 0.6, 0.8, 1.0 };

		final ArrayList<Object[]> parameters = new ArrayList<Object[]>();
		for (final EvaluationExpression projection : projections)
			for (final double threshold : thresholds)
				parameters.add(new Object[] { threshold, projection });

		return parameters;
	}
}
