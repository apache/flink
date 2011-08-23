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
 * Tests {@link Naive} {@link InterSourceRecordLinkage} with two data sources.
 * 
 * @author Arvid Heise
 */
public class NaiveRecordLinkageInterSourceTest extends InterSourceRecordLinkageAlgorithmTestBase<Naive> {

	/**
	 * Initializes NaiveRecordLinkageInterSourceTest with the given parameter
	 * 
	 * @param resultProjection1
	 * @param resultProjection2
	 */
	public NaiveRecordLinkageInterSourceTest(final EvaluationExpression resultProjection1,
			final EvaluationExpression resultProjection2) {
		super(resultProjection1, resultProjection2);
	}

	@Override
	protected void generateExpectedPairs(Input leftInput, Input rightInput) {
		for (final KeyValuePair<Key, PactJsonObject> left : leftInput)
			for (final KeyValuePair<Key, PactJsonObject> right : rightInput)
				emitCandidate(left, right);
	}

	@Override
	protected RecordLinkageAlgorithm createAlgorithm() {
		return new Naive();
	}

	/**
	 * Returns the parameter combination under test.
	 * 
	 * @return the parameter combination
	 */
	@Parameters
	public static Collection<Object[]> getParameters() {
		final EvaluationExpression[][] projections = { { null, null },
			{ getAggregativeProjection1(), getAggregativeProjection2() }, };

		final ArrayList<Object[]> parameters = new ArrayList<Object[]>();
		for (final EvaluationExpression[] projection : projections)
			parameters.add(new Object[] { projection[0], projection[1] });

		return parameters;
	}
}
