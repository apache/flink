package eu.stratosphere.sopremo.cleansing.record_linkage;

import static eu.stratosphere.sopremo.SopremoTest.createPactJsonArray;
import static eu.stratosphere.sopremo.SopremoTest.createPactJsonObject;
import static eu.stratosphere.sopremo.SopremoTest.createPath;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.shef.wit.simmetrics.similaritymetrics.JaccardSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.Levenshtein;
import eu.stratosphere.sopremo.base.BuiltinFunctions;
import eu.stratosphere.sopremo.cleansing.similarity.NumericDifference;
import eu.stratosphere.sopremo.cleansing.similarity.SimmetricFunction;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class IntraSourceRecordLinkageTest {

	// test transitive

	// test cluster

	// test threshold

	// test 1, 2, 3, 4 inputs

	private PathExpression similarityFunction;

	private List<PactJsonObject> inputs = new ArrayList<PactJsonObject>();

	public IntraSourceRecordLinkageTest() {
		final SimmetricFunction firstNameLev = new SimmetricFunction(new Levenshtein(),
			createPath("0", "first name"), createPath("1", "first name"));
		final SimmetricFunction lastNameJaccard = new SimmetricFunction(new Levenshtein(),
			createPath("0", "last name"), createPath("1", "last name"));
		final EvaluationExpression ageDiff = new NumericDifference(createPath("0", "age"), createPath("1", "age"), 10);
		final ArrayCreation fieldSimExpr = new ArrayCreation(firstNameLev, lastNameJaccard, ageDiff);
		this.similarityFunction = new PathExpression(fieldSimExpr, BuiltinFunctions.AVERAGE.asExpression());

		inputs.add(createPactJsonObject("id", 0, "first name", "albert", "last name", "perfect duplicate", "age", 80));
		inputs.add(createPactJsonObject("id", 1, "first name", "berta", "last name", "typo", "age", 70));
		inputs.add(createPactJsonObject("id", 2, "first name", "charles", "last name", "age inaccurate", "age", 70));
		inputs.add(createPactJsonObject("id", 3, "first name", "dagmar", "last name", "unmatched", "age", 75));
		inputs.add(createPactJsonObject("id", 4, "first name", "elma", "last name", "first nameDiffers", "age", 60));
		inputs.add(createPactJsonObject("id", 5, "first name", "albert", "last name", "perfect duplicate", "age", 80));
		inputs.add(createPactJsonObject("id", 6, "first name", "berta", "last name", "tpyo", "age", 70));
		inputs.add(createPactJsonObject("id", 7, "first name", "charles", "last name", "age inaccurate", "age", 69));
		inputs.add(createPactJsonObject("id", 8, "first name", "elmar", "last name", "first nameDiffers", "age", 60));
		inputs.add(createPactJsonObject("id", 9, "first name", "frank", "last name", "transitive", "age", 65));
		inputs.add(createPactJsonObject("id", 10, "first name", "frank", "last name", "transitive", "age", 60));
		inputs.add(createPactJsonObject("id", 11, "first name", "frank", "last name", "transitive", "age", 70));
	}

	private EvaluationExpression resultProjection = null;

	@Test
	public void shouldFindLinksOnly() {
		final SopremoTestPlan testPlan = createTestPlan(LinkageMode.LINKS_ONLY, false);

		testPlan.getExpectedOutput(0).
			add(arrayOfElement(testPlan, 0, 5)).
			add(arrayOfElement(testPlan, 1, 6)).
			add(arrayOfElement(testPlan, 2, 7)).
			add(arrayOfElement(testPlan, 4, 8)).
			add(arrayOfElement(testPlan, 9, 10)).
			add(arrayOfElement(testPlan, 9, 11));

		testPlan.run();
	}

	@Test
	public void shouldFindAdditionalLinks() {
		final SopremoTestPlan testPlan = createTestPlan(LinkageMode.TRANSITIVE_LINKS, false);

		testPlan.getExpectedOutput(0).
			add(arrayOfElement(testPlan, 0, 5)).
			add(arrayOfElement(testPlan, 1, 6)).
			add(arrayOfElement(testPlan, 2, 7)).
			add(arrayOfElement(testPlan, 4, 8)).
			add(arrayOfElement(testPlan, 9, 10)).
			add(arrayOfElement(testPlan, 9, 11)).
			add(arrayOfElement(testPlan, 10, 11)); // <-- new

		testPlan.run();
	}
	
	@Test
	public void shouldFindClusters() {
		final SopremoTestPlan testPlan = createTestPlan(LinkageMode.DUPLICATE_CLUSTERS, false);

		testPlan.getExpectedOutput(0).
			add(arrayOfElement(testPlan, 0, 5)).
			add(arrayOfElement(testPlan, 1, 6)).
			add(arrayOfElement(testPlan, 2, 7)).
			add(arrayOfElement(testPlan, 4, 8)).
			add(arrayOfElement(testPlan, 9, 10, 11)); // <-- cluster

		testPlan.run();
	}
	
	@Test
	public void shouldAddSinglesClusters() {
		final SopremoTestPlan testPlan = createTestPlan(LinkageMode.ALL_CLUSTERS, false);

		testPlan.getExpectedOutput(0).
			add(arrayOfElement(testPlan, 0, 5)).
			add(arrayOfElement(testPlan, 1, 6)).
			add(arrayOfElement(testPlan, 2, 7)).
			add(arrayOfElement(testPlan, 4, 8)).
			add(arrayOfElement(testPlan, 9, 10, 11)).
			add(arrayOfElement(testPlan, 3)); // <-- single node

		testPlan.run();
	}

	private PactJsonObject arrayOfElement(SopremoTestPlan testPlan, int... ids) {
		Object[] array = new JsonNode[ids.length];
		EvaluationExpression resultProjection = this.resultProjection;
		if (resultProjection == null)
			resultProjection = EvaluationExpression.VALUE;
			
		for (int index = 0; index < array.length; index++)
			array[index] = resultProjection
				.evaluate(inputs.get(ids[index]).getValue(), testPlan.getEvaluationContext());
		return createPactJsonArray(array);
	}

	protected SopremoTestPlan createTestPlan(final LinkageMode mode, final boolean useId) {

		IntraSourceRecordLinkage recordLinkage = new IntraSourceRecordLinkage(new Naive(), similarityFunction, 0.7, null);
		recordLinkage.setLinkageMode(mode);
		final SopremoTestPlan sopremoTestPlan = new SopremoTestPlan(recordLinkage);
		if (useId) {
			recordLinkage.getRecordLinkageInput().setIdProjection(new ObjectAccess("id"));
		}
		if (resultProjection != null)
			recordLinkage.getRecordLinkageInput().setResultProjection(resultProjection);

		for (PactJsonObject object : inputs)
			sopremoTestPlan.getInput(0).
				add(object);
		return sopremoTestPlan;
	}

}
