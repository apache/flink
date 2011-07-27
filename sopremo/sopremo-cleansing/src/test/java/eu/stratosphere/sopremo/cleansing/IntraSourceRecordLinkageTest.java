package eu.stratosphere.sopremo.cleansing;

import static eu.stratosphere.sopremo.SopremoTest.createArrayNode;
import static eu.stratosphere.sopremo.SopremoTest.createPactJsonObject;
import static eu.stratosphere.sopremo.SopremoTest.createPath;

import org.codehaus.jackson.JsonNode;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import uk.ac.shef.wit.simmetrics.similaritymetrics.JaccardSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.Levenshtein;
import eu.stratosphere.sopremo.base.BuiltinFunctions;
import eu.stratosphere.sopremo.cleansing.RecordLinkage.Partitioning;
import eu.stratosphere.sopremo.cleansing.similarity.NumericDifference;
import eu.stratosphere.sopremo.cleansing.similarity.SimmetricFunction;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.pact.JsonNodeComparator;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

/**
 * Base for inner source {@link RecordLinkage} test cases within one source.
 * 
 * @author Arvid Heise
 * @param <P>
 *        the {@link Partitioning}
 */
@RunWith(Parameterized.class)
@Ignore
public class IntraSourceRecordLinkageTest<P extends Partitioning> {
	/**
	 * Returns the similarity function for the test case.
	 * 
	 * @return the similarity function
	 */
	protected EvaluationExpression getSimilarityFunction() {
		SimmetricFunction firstNameLev = new SimmetricFunction(new Levenshtein(),
			createPath("0", "first name"), createPath("1", "first name"));
		SimmetricFunction lastNameJaccard = new SimmetricFunction(new JaccardSimilarity(),
			createPath("0", "last name"), createPath("1", "last name"));
		EvaluationExpression ageDiff = new NumericDifference(createPath("0", "age"), createPath("0", "age"), 10);
		ArrayCreation fieldSimExpr = new ArrayCreation(firstNameLev, lastNameJaccard, ageDiff);
		EvaluationExpression simExpr = new PathExpression(fieldSimExpr, BuiltinFunctions.AVERAGE.asExpression());
		return simExpr;
	}

	/**
	 * Returns a duplicate projection expression that aggregates some fields to arrays.
	 * 
	 * @return an aggregating expression
	 */
	protected static EvaluationExpression getAggregativeProjection() {
		ObjectCreation aggregating = new ObjectCreation();
		aggregating.addMapping("names",
			new ArrayCreation(createPath("0", "first name"), createPath("1", "first name")));
		aggregating.addMapping("ids", new ArrayCreation(createPath("0", "id"), createPath("1", "id")));

		return aggregating;
	}

	/**
	 * Creates a test plan for the record linkage operator.
	 * 
	 * @param recordLinkage
	 * @param useId
	 * @param projection
	 * @return the generated test plan
	 */
	protected SopremoTestPlan createTestPlan(RecordLinkage recordLinkage, boolean useId, EvaluationExpression projection) {
		SopremoTestPlan sopremoTestPlan = new SopremoTestPlan(recordLinkage);
		if (useId)
			recordLinkage.setIdProjection(0, new ObjectAccess("id"));
		if (projection != null)
			recordLinkage.setDuplicateProjection(projection);

		sopremoTestPlan.getInput(0).
			add(createPactJsonObject("id", 0, "first name", "albert", "last name", "perfect duplicate", "age", 80)).
			add(createPactJsonObject("id", 1, "first name", "berta", "last name", "typo", "age", 70)).
			add(createPactJsonObject("id", 2, "first name", "charles", "last name", "age inaccurate", "age", 70)).
			add(createPactJsonObject("id", 3, "first name", "dagmar", "last name", "unmatched", "age", 75)).
			add(createPactJsonObject("id", 4, "first name", "elma", "last name", "first nameDiffers", "age", 60)).
			add(createPactJsonObject("id", 5, "first name", "albert", "last name", "perfect duplicate", "age", 80)).
			add(createPactJsonObject("id", 6, "first name", "berta", "last name", "tpyo", "age", 70)).
			add(createPactJsonObject("id", 7, "first name", "charles", "last name", "age inaccurate", "age", 69)).
			add(createPactJsonObject("id", 8, "first name", "elmar", "last name", "first nameDiffers", "age", 60));
		return sopremoTestPlan;
	}

	/**
	 * Creates an ordered pair as produced in intra source duplicate detection without explicit projection.
	 * 
	 * @param o1
	 *        the first node
	 * @param o2
	 *        the second node
	 * @return the ordered pair of the given nodes
	 */
	protected JsonNode createOrderedPair(JsonNode o1, JsonNode o2) {
		if (JsonNodeComparator.INSTANCE.compare(o1, o2) < 0)
			return createArrayNode(o1, o2);
		return createArrayNode(o2, o1);
	}
}
