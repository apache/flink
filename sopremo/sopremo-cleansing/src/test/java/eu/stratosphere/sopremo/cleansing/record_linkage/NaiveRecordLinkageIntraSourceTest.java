package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
import eu.stratosphere.sopremo.testing.SopremoTestPlan.Input;

/**
 * Tests {@link Naive} {@link InterSourceRecordLinkage} with one data source.
 * 
 * @author Arvid Heise
 */
public class NaiveRecordLinkageIntraSourceTest extends IntraSourceRecordLinkageTestBase<Naive> {

	/**
	 * Initializes NaiveRecordLinkageIntraSourceTest.
	 * 
	 * @param resultProjection
	 * @param useId
	 */
	public NaiveRecordLinkageIntraSourceTest(
			final EvaluationExpression resultProjection, final boolean useId) {
		super(resultProjection, useId);
	}

	@Override
	protected RecordLinkageAlgorithm createAlgorithm() {
		return new Naive();
	}

	@Override
	protected void generateExpectedPairs(Input input) {
		for (final KeyValuePair<Key, PactJsonObject> left : input) {
			boolean skipPairs = true;
			for (final KeyValuePair<Key, PactJsonObject> right : input) {
				if (left == right) {
					skipPairs = false;
					continue;
				} else if (skipPairs)
					continue;

				emitCandidate(left, right);
			}
		}
	}

	/**
	 * Returns the parameter combination under test.
	 * 
	 * @return the parameter combination
	 */
	@Parameters
	public static Collection<Object[]> getParameters() {
		final EvaluationExpression[] projections = { null, getAggregativeProjection() };
		final boolean[] useIds = { false, true };

		final ArrayList<Object[]> parameters = new ArrayList<Object[]>();
		for (final EvaluationExpression projection : projections)
			for (final boolean useId : useIds)
				parameters.add(new Object[] { projection, useId });

		return parameters;
	}
}
