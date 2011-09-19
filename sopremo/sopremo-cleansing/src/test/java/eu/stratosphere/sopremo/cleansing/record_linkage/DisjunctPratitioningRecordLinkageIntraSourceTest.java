package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
import eu.stratosphere.sopremo.testing.SopremoTestPlan.Input;

/**
 * Tests {@link DisjunctPartitioning} {@link InterSourceRecordLinkage} within one data source.
 * 
 * @author Arvid Heise
 */
public class DisjunctPratitioningRecordLinkageIntraSourceTest extends
		IntraSourceRecordLinkageTestBase<DisjunctPartitioning> {
	private final EvaluationExpression[] blockingKeys;

	/**
	 * Initializes NaiveRecordLinkageInterSourceTest with the given parameter
	 * 
	 * @param projection
	 * @param useId
	 * @param blockingKeys
	 */
	public DisjunctPratitioningRecordLinkageIntraSourceTest(final EvaluationExpression projection,
			final boolean useId, final String[][] blockingKeys) {
		super(projection, useId);

		this.blockingKeys = new EvaluationExpression[blockingKeys[0].length];
		for (int index = 0; index < this.blockingKeys.length; index++)
			this.blockingKeys[index] = new ObjectAccess(blockingKeys[0][index]);
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

				boolean inSameBlockingBin = false;
				for (int index = 0; index < this.blockingKeys.length && !inSameBlockingBin; index++)
					if (this.blockingKeys[index].evaluate(left.getValue().getValue(), this.getContext()).equals(
						this.blockingKeys[index].evaluate(right.getValue().getValue(), this.getContext())))
						inSameBlockingBin = true;
				if (inSameBlockingBin)
					this.emitCandidate(left, right);
			}
		}
	}

	@Override
	protected RecordLinkageAlgorithm createAlgorithm() {
		return new DisjunctPartitioning(this.blockingKeys, this.blockingKeys);
	}

	@Override
	public String toString() {
		return String.format("%s, blockingKeys=%s", super.toString(), Arrays.toString(this.blockingKeys));
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
			for (final String[][] combinedBlockingKeys : CombinedBlockingKeys)
				for (final boolean useId : useIds)
					parameters.add(new Object[] { projection, useId, combinedBlockingKeys });

		return parameters;
	}

}
