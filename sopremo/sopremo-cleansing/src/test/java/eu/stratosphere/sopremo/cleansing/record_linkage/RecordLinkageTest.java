package eu.stratosphere.sopremo.cleansing.record_linkage;

import static eu.stratosphere.sopremo.SopremoTest.createPactJsonObject;
import static eu.stratosphere.sopremo.SopremoTest.createPath;

import org.junit.Ignore;

import uk.ac.shef.wit.simmetrics.similaritymetrics.JaccardSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.Levenshtein;

import eu.stratosphere.sopremo.base.BuiltinFunctions;
import eu.stratosphere.sopremo.cleansing.similarity.NumericDifference;
import eu.stratosphere.sopremo.cleansing.similarity.SimmetricFunction;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

@Ignore
public class RecordLinkageTest {

	// test transitive

	// test cluster

	// test threshold

	// test 1, 2, 3, 4 inputs

	private PathExpression similarityFunction;

	public RecordLinkageTest() {
		final SimmetricFunction firstNameLev = new SimmetricFunction(new Levenshtein(),
			createPath("0", "first name"), createPath("1", "firstName"));
		final SimmetricFunction lastNameJaccard = new SimmetricFunction(new JaccardSimilarity(),
			createPath("0", "last name"), createPath("1", "lastName"));
		final EvaluationExpression ageDiff = new NumericDifference(createPath("0", "age"), createPath("1", "age"), 10);
		final ArrayCreation fieldSimExpr = new ArrayCreation(firstNameLev, lastNameJaccard, ageDiff);
		this.similarityFunction = new PathExpression(fieldSimExpr, BuiltinFunctions.AVERAGE.asExpression());

	}

	protected static SopremoTestPlan createTestPlan(final RecordLinkage recordLinkage, final boolean useId,
			final EvaluationExpression resultProjection1, final EvaluationExpression resultProjection2) {
		final SopremoTestPlan sopremoTestPlan = new SopremoTestPlan(recordLinkage);
		if (useId) {
			recordLinkage.getRecordLinkageInput(0).setIdProjection(new ObjectAccess("id"));
			recordLinkage.getRecordLinkageInput(1).setIdProjection(new ObjectAccess("id2"));
		}
		if (resultProjection1 != null)
			recordLinkage.getRecordLinkageInput(0).setResultProjection(resultProjection1);
		if (resultProjection2 != null)
			recordLinkage.getRecordLinkageInput(1).setResultProjection(resultProjection2);

		sopremoTestPlan.getInput(0).
			add(createPactJsonObject("id", 0, "first name", "albert", "last name", "perfect duplicate", "age", 80)).
			add(createPactJsonObject("id", 1, "first name", "berta", "last name", "typo", "age", 70)).
			add(createPactJsonObject("id", 2, "first name", "charles", "last name", "age inaccurate", "age", 70)).
			add(createPactJsonObject("id", 3, "first name", "dagmar", "last name", "unmatched", "age", 75)).
			add(createPactJsonObject("id", 4, "first name", "elma", "last name", "firstNameDiffers", "age", 60));
		sopremoTestPlan.getInput(1).
			add(createPactJsonObject("id2", 10, "firstName", "albert", "lastName", "perfect duplicate", "age", 80)).
			add(createPactJsonObject("id2", 11, "firstName", "berta", "lastName", "tpyo", "age", 70)).
			add(createPactJsonObject("id2", 12, "firstName", "charles", "lastName", "age inaccurate", "age", 69)).
			add(createPactJsonObject("id2", 14, "firstName", "elmar", "lastName", "firstNameDiffers", "age", 60));
		return sopremoTestPlan;
	}

}
