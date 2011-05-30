package eu.stratosphere.sopremo.operator;

import java.util.Arrays;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.SopremoTestPlan;
import eu.stratosphere.sopremo.base.Aggregation;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.ObjectCreation;

public class AggregationTest extends SopremoTest {

	@Test
	public void shouldGroupTwoSources() {

		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("dept", createPath("0", "dept", "[0]"));
		transformation.addMapping("deptName", createPath("1", "[0]", "name"));
		transformation.addMapping("emps", createPath("0", "[*]", "id"));
		transformation.addMapping("numEmps", new FunctionCall("count", createPath("0", "dept")));
		sopremoPlan.getOutputOperator(0).setInputOperators(
			new Aggregation(transformation, Arrays.asList(createPath("0", "dept"), createPath("1", "did")), sopremoPlan
				.getInputOperators(0, 2)));

		sopremoPlan.getInput(0).
			add(createJsonObject("id", 1, "dept", 1, "income", 12000)).
			add(createJsonObject("id", 2, "dept", 1, "income", 13000)).
			add(createJsonObject("id", 3, "dept", 2, "income", 15000)).
			add(createJsonObject("id", 4, "dept", 1, "income", 10000)).
			add(createJsonObject("id", 5, "dept", 3, "income", 8000)).
			add(createJsonObject("id", 6, "dept", 2, "income", 5000)).
			add(createJsonObject("id", 7, "dept", 1, "income", 24000));
		sopremoPlan.getInput(1).
			add(createJsonObject("did", 1, "name", "development")).
			add(createJsonObject("did", 2, "name", "marketing")).
			add(createJsonObject("did", 3, "name", "sales"));
		sopremoPlan.getExpectedOutput(0).
			add(
				createJsonObject("dept", 1, "deptName", "development", "emps", new int[] { 1, 2, 4, 7 },
					"numEmps", 4)).
			add(
				createJsonObject("dept", 2, "deptName", "marketing", "emps", new int[] { 3, 6 }, "numEmps", 2)).
			add(createJsonObject("dept", 3, "deptName", "sales", "emps", new int[] { 5 }, "numEmps", 1));

		sopremoPlan.run();
	}

	@Test
	public void shouldGroupWithSingleSource() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("d", createPath("dept", "[0]"));
		transformation.addMapping("total", new FunctionCall("sum", createPath("[*]", "income")));
		sopremoPlan.getOutputOperator(0).setInputOperators(
			new Aggregation(transformation, Arrays.asList(createPath("dept")), sopremoPlan.getInputOperator(0)));

		sopremoPlan.getInput(0).
			add(createJsonObject("id", 1, "dept", 1, "income", 12000)).
			add(createJsonObject("id", 2, "dept", 1, "income", 13000)).
			add(createJsonObject("id", 3, "dept", 2, "income", 15000)).
			add(createJsonObject("id", 4, "dept", 1, "income", 10000)).
			add(createJsonObject("id", 5, "dept", 3, "income", 8000)).
			add(createJsonObject("id", 6, "dept", 2, "income", 5000)).
			add(createJsonObject("id", 7, "dept", 1, "income", 24000));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonObject("d", 1, "total", 59000)).
			add(createJsonObject("d", 2, "total", 20000)).
			add(createJsonObject("d", 3, "total", 8000));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformSimpleGroupBy() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		sopremoPlan.getOutputOperator(0).setInputOperators(
			new Aggregation(new FunctionCall("count", EvaluableExpression.IDENTITY), Aggregation.NO_GROUPING,
				sopremoPlan
					.getInputOperator(0)));

		sopremoPlan.getInput(0).
			add(createJsonObject("id", 1, "dept", 1, "income", 12000)).
			add(createJsonObject("id", 2, "dept", 1, "income", 13000)).
			add(createJsonObject("id", 3, "dept", 2, "income", 15000)).
			add(createJsonObject("id", 4, "dept", 1, "income", 10000)).
			add(createJsonObject("id", 5, "dept", 3, "income", 8000)).
			add(createJsonObject("id", 6, "dept", 2, "income", 5000)).
			add(createJsonObject("id", 7, "dept", 1, "income", 24000));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonValue(7));

		sopremoPlan.run();
	}
}
