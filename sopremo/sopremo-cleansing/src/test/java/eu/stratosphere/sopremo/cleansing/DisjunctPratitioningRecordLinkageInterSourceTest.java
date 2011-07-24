package eu.stratosphere.sopremo.cleansing;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.sopremo.CompactArrayNode;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.SopremoTestPlan;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;

/**
 * Tests {@link DisjunctPartitioning} {@link RecordLinkage} with two data sources.
 * 
 * @author Arvid Heise
 */
public class DisjunctPratitioningRecordLinkageInterSourceTest extends
		InterSourceRecordLinkageTest<DisjunctPartitioning> {

	private static String[][] BlockingKeys = new String[][] {
		{ "first name", "firstName" },
		{ "last name", "lastName" },
		{ "age", "age" },
	};

	private static String[][][] CombinedBlockingKeys = new String[][][] {
		{ { BlockingKeys[0][0] }, { BlockingKeys[0][1] } },
		{ { BlockingKeys[1][0] }, { BlockingKeys[1][1] } },
		{ { BlockingKeys[2][0] }, { BlockingKeys[2][1] } },

		{ { BlockingKeys[0][0], BlockingKeys[1][0] }, { BlockingKeys[0][1], BlockingKeys[1][1] } },
		{ { BlockingKeys[0][0], BlockingKeys[2][0] }, { BlockingKeys[0][1], BlockingKeys[2][1] } },
		{ { BlockingKeys[1][0], BlockingKeys[2][0] }, { BlockingKeys[1][1], BlockingKeys[2][1] } },

		{ { BlockingKeys[0][0], BlockingKeys[1][0], BlockingKeys[2][0] },
			{ BlockingKeys[0][1], BlockingKeys[1][1], BlockingKeys[2][1] } },
	};

	private double threshold;

	private EvaluationExpression projection;

	private EvaluationExpression leftBlocking, rightBlocking;

	/**
	 * Initializes NaiveRecordLinkageInterSourceTest with the given parameter
	 * 
	 * @param threshold
	 * @param projection
	 * @param blockingKeys
	 */
	public DisjunctPratitioningRecordLinkageInterSourceTest(double threshold, EvaluationExpression projection,
			String[][] blockingKeys) {
		this.threshold = threshold;
		this.projection = projection;

		EvaluationExpression[] leftKeys = new EvaluationExpression[blockingKeys[0].length];
		for (int index = 0; index < leftKeys.length; index++)
			leftKeys[index] = new ObjectAccess(blockingKeys[0][index]);
		EvaluationExpression[] rightKeys = new EvaluationExpression[blockingKeys[1].length];
		for (int index = 0; index < rightKeys.length; index++)
			rightKeys[index] = new ObjectAccess(blockingKeys[1][index]);

		this.leftBlocking = new ArrayCreation(leftKeys);
		this.rightBlocking = new ArrayCreation(rightKeys);
	}

	/**
	 * Returns the parameter combination under test.
	 * 
	 * @return the parameter combination
	 */
	@Parameters
	public static Collection<Object[]> getParameters() {
		EvaluationExpression[] projections = { null, getAggregativeProjection() };
		double[] thresholds = { 0.0, 0.4, 0.8, 1.0 };

		ArrayList<Object[]> parameters = new ArrayList<Object[]>();
		for (EvaluationExpression projection : projections)
			for (double threshold : thresholds)
				for (String[][] combinedBlockingKeys : CombinedBlockingKeys)
					parameters.add(new Object[] { threshold, projection, combinedBlockingKeys });

		return parameters;
	}

	/**
	 * Performs the naive record linkage in place and compares with the Pact code.
	 */
	@Test
	public void pactCodeShouldPerformLikeStandardImplementation() {
		EvaluationExpression similarityFunction = getSimilarityFunction();
		RecordLinkage recordLinkage = new RecordLinkage(new DisjunctPartitioning(this.leftBlocking, this.rightBlocking),
			similarityFunction, this.threshold, null, null);
		SopremoTestPlan sopremoTestPlan = createTestPlan(recordLinkage, false, this.projection);

		EvaluationExpression duplicateProjection = this.projection;
		if (duplicateProjection == null)
			duplicateProjection = new ArrayCreation(new PathExpression(new InputSelection(0),
				recordLinkage.getIdProjection(0)), new PathExpression(new InputSelection(1),
				recordLinkage.getIdProjection(1)));

		EvaluationContext context = sopremoTestPlan.getEvaluationContext();
		for (KeyValuePair<Key, PactJsonObject> left : sopremoTestPlan.getInput(0))
			for (KeyValuePair<Key, PactJsonObject> right : sopremoTestPlan.getInput(1)) {
				if (!this.leftBlocking.evaluate(left.getValue().getValue(), context).equals(
					this.rightBlocking.evaluate(right.getValue().getValue(), context)))
					continue;
				CompactArrayNode pair = JsonUtil.asArray(left.getValue().getValue(), right.getValue().getValue());
				if (similarityFunction.evaluate(pair, context).getDoubleValue() > this.threshold)
					sopremoTestPlan.getExpectedOutput(0).add(
						new PactJsonObject(duplicateProjection.evaluate(pair, context)));
			}
		sopremoTestPlan.run();
	}
}
