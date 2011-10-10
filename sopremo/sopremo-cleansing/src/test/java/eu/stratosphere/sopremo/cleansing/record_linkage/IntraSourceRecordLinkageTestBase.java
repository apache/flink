package eu.stratosphere.sopremo.cleansing.record_linkage;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.testing.SopremoTestPlan.Input;

/**
 * Base for inner source {@link InterSourceRecordLinkage} test cases within one source.
 * 
 * @author Arvid Heise
 * @param <P>
 *        the {@link RecordLinkageAlgorithm}
 */
@RunWith(Parameterized.class)
@Ignore
public abstract class IntraSourceRecordLinkageTestBase<P extends RecordLinkageAlgorithm> extends
		RecordLinkageAlgorithmTestBase {
	private final EvaluationExpression resultProjection;

	private final boolean useId;

	private SopremoTestPlan sopremoTestPlan;

	/**
	 * Initializes IntraSourceRecordLinkageTestBase.
	 * 
	 * @param resultProjection
	 * @param useId
	 */
	public IntraSourceRecordLinkageTestBase(EvaluationExpression resultProjection, boolean useId) {
		this.resultProjection = resultProjection;
		this.useId = useId;
	}

	/**
	 * Performs the naive record linkage in place and compares with the Pact code.
	 */
	@Test
	public void pactCodeShouldPerformLikeStandardImplementation() {
		final IntraSourceRecordLinkage recordLinkage = new IntraSourceRecordLinkage();
		recordLinkage.setAlgorithm(this.createAlgorithm());
		this.sopremoTestPlan = this.createTestPlan(recordLinkage, this.useId, this.resultProjection);

		EvaluationExpression resultProjection = this.resultProjection;
		if (resultProjection == null)
			resultProjection = EvaluationExpression.VALUE;

		this.generateExpectedPairs(this.sopremoTestPlan.getInput(0));

		try {
			this.sopremoTestPlan.run();
		} catch (final AssertionError error) {
			throw new AssertionError(String.format("For test %s: %s", this, error.getMessage()));
		}
	}

	/**
	 * Generates the expected pairs and invokes {@link #emitCandidate(KeyValuePair, KeyValuePair)}.
	 * 
	 * @param input
	 */
	protected abstract void generateExpectedPairs(Input input);

	/**
	 * Emit the candidate.
	 * 
	 * @param left
	 * @param right
	 */
	protected void emitCandidate(KeyValuePair<JsonNode, JsonNode> left, KeyValuePair<JsonNode, JsonNode> right) {
		EvaluationExpression resultProjection = this.resultProjection;
		if (resultProjection == null)
			resultProjection = EvaluationExpression.VALUE;

		final EvaluationContext context = this.getContext();

		JsonNode smaller = left.getValue(), bigger = right.getValue();
		if (bigger.compareTo(smaller) < 0) {
			JsonNode temp = smaller;
			smaller = bigger;
			bigger = temp;
		}
		this.sopremoTestPlan.getExpectedOutput(0).addArray(resultProjection.evaluate(smaller, context),
			resultProjection.evaluate(bigger, context));
	}

	/**
	 * Returns the context of the test plan.
	 * 
	 * @return the context
	 */
	protected EvaluationContext getContext() {
		return this.sopremoTestPlan.getEvaluationContext();
	}

	/**
	 * Creates the algorithm with the similarityFunction and threshold
	 * 
	 * @return the configured algorithm
	 */
	protected abstract RecordLinkageAlgorithm createAlgorithm();

	/**
	 * Creates a test plan for the record linkage operator.
	 * 
	 * @param recordLinkage
	 * @param useId
	 * @param projection
	 * @return the generated test plan
	 */
	protected SopremoTestPlan createTestPlan(final IntraSourceRecordLinkage recordLinkage, final boolean useId,
			final EvaluationExpression projection) {
		final SopremoTestPlan sopremoTestPlan = new SopremoTestPlan(recordLinkage);
		if (useId)
			recordLinkage.getRecordLinkageInput().setIdProjection(new ObjectAccess("id"));
		if (projection != null)
			recordLinkage.getRecordLinkageInput().setResultProjection(projection);
		sopremoTestPlan.getInput(0).
			addObject("id", 0, "first name", "albert", "last name", "perfect duplicate", "age", 80).
			addObject("id", 1, "first name", "berta", "last name", "typo", "age", 70).
			addObject("id", 2, "first name", "charles", "last name", "age inaccurate", "age", 70).
			addObject("id", 3, "first name", "dagmar", "last name", "unmatched", "age", 75).
			addObject("id", 4, "first name", "elma", "last name", "first nameDiffers", "age", 60).
			addObject("id", 5, "first name", "albert", "last name", "perfect duplicate", "age", 80).
			addObject("id", 6, "first name", "berta", "last name", "tpyo", "age", 70).
			addObject("id", 7, "first name", "charles", "last name", "age inaccurate", "age", 69).
			addObject("id", 8, "first name", "elmar", "last name", "first nameDiffers", "age", 60);
		return sopremoTestPlan;
	}

	/**
	 * Returns a duplicate projection expression that aggregates some fields to arrays.
	 * 
	 * @return an aggregating expression
	 */
	protected static EvaluationExpression getAggregativeProjection() {
		final ObjectCreation aggregating = new ObjectCreation();
		aggregating.addMapping("name", new ObjectAccess("first name"));
		aggregating.addMapping("id", new ObjectAccess("id"));

		return aggregating;
	}
}
