package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.testing.SopremoTestPlan.Input;

/**
 * Tests {@link DisjunctPartitioning} {@link InterSourceRecordLinkage} with two data sources.
 * 
 * @author Arvid Heise
 */
public class DisjunctPratitioningRecordLinkageInterSourceTest extends
		InterSourceRecordLinkageAlgorithmTestBase<DisjunctPartitioning> {

	private final EvaluationExpression[] leftBlockingKeys, rightBlockingKeys;

	/**
	 * Initializes NaiveRecordLinkageInterSourceTest with the given parameter
	 * 
	 * @param resultProjection1
	 * @param resultProjection2
	 * @param blockingKeys
	 */
	public DisjunctPratitioningRecordLinkageInterSourceTest(final EvaluationExpression resultProjection1,
			final EvaluationExpression resultProjection2, final String[][] blockingKeys) {
		super(resultProjection1, resultProjection2);

		this.leftBlockingKeys = new EvaluationExpression[blockingKeys[0].length];
		for (int index = 0; index < this.leftBlockingKeys.length; index++)
			this.leftBlockingKeys[index] = new ObjectAccess(blockingKeys[0][index]);
		this.rightBlockingKeys = new EvaluationExpression[blockingKeys[1].length];
		for (int index = 0; index < this.rightBlockingKeys.length; index++)
			this.rightBlockingKeys[index] = new ObjectAccess(blockingKeys[1][index]);

	}

	@Override
	protected void generateExpectedPairs(Input leftInput, Input rightInput) {
		for (final KeyValuePair<JsonNode, JsonNode> left : leftInput)
			for (final KeyValuePair<JsonNode, JsonNode> right : rightInput) {
				boolean inSameBlockingBin = false;
				for (int index = 0; index < this.leftBlockingKeys.length && !inSameBlockingBin; index++)
					if (this.leftBlockingKeys[index].evaluate(left.getValue(), this.getContext()).equals(
						this.rightBlockingKeys[index].evaluate(right.getValue(), this.getContext())))
						inSameBlockingBin = true;
				if (inSameBlockingBin)
					this.emitCandidate(left, right);
			}

	}

	@Override
	protected RecordLinkageAlgorithm createAlgorithm() {
		return new DisjunctPartitioning(this.leftBlockingKeys, this.rightBlockingKeys);
	}

	@Override
	public String toString() {
		return String.format("%s, leftBlockingKeys=%s, rightBlockingKeys=%s", super.toString(),
			Arrays.toString(this.leftBlockingKeys), Arrays.toString(this.rightBlockingKeys));
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
			for (final String[][] combinedBlockingKeys : CombinedBlockingKeys)
				parameters.add(new Object[] { projection[0], projection[1], combinedBlockingKeys });

		return parameters;
	}
}
