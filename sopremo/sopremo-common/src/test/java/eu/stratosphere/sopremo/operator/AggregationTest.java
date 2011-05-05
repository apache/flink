package eu.stratosphere.sopremo.operator;

import java.util.Arrays;

import org.junit.Test;

import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.sopremo.BooleanExpression;
import eu.stratosphere.sopremo.Comparison;
import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Condition.Combination;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.SopremoTestPlan;
import eu.stratosphere.sopremo.Comparison.BinaryOperator;
import eu.stratosphere.sopremo.UnaryExpression;
import eu.stratosphere.sopremo.expressions.Arithmetic;
import eu.stratosphere.sopremo.expressions.Function;
import eu.stratosphere.sopremo.expressions.Input;
import eu.stratosphere.sopremo.expressions.Path;
import eu.stratosphere.sopremo.expressions.Transformation;
import eu.stratosphere.sopremo.expressions.ValueAssignment;
import eu.stratosphere.sopremo.expressions.Arithmetic.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.Constant;

public class AggregationTest extends SopremoTest {

	@Test
	public void shouldGroupTwoSources() {

		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment("dept", createPath("0", "dept")));
		transformation.addMapping(new ValueAssignment("deptName", createPath("1", "[0]", "name")));
		transformation.addMapping(new ValueAssignment("emps", createPath("0", "[*]", "id")));
		transformation.addMapping(new ValueAssignment("numEmps", new Function("count", createPath("0", "dept"))));
		sopremoPlan.getOutputOperator(0).setInputOperators(
			new Aggregation(transformation, Arrays.asList(createPath("0", "dept"), createPath("1", "did")), sopremoPlan
				.getInputOperators(0, 2)));

		sopremoPlan.getInput(0).
			add(createJsonObject("id", 1L, "dept", 1L, "income", 12000L)).
			add(createJsonObject("id", 2L, "dept", 1L, "income", 13000L)).
			add(createJsonObject("id", 3L, "dept", 2L, "income", 15000L)).
			add(createJsonObject("id", 4L, "dept", 1L, "income", 10000L)).
			add(createJsonObject("id", 5L, "dept", 3L, "income", 8000L)).
			add(createJsonObject("id", 6L, "dept", 2L, "income", 5000L)).
			add(createJsonObject("id", 7L, "dept", 1L, "income", 24000L));
		sopremoPlan.getInput(1).
			add(createJsonObject("did", 1L, "name", "development")).
			add(createJsonObject("did", 2L, "name", "marketing")).
			add(createJsonObject("did", 3L, "name", "sales"));
		sopremoPlan.getOutput(0).
			addExpected(
				createJsonObject("dept", 1L, "deptName", "development", "emps", new long[] { 1L, 2L, 4L, 7L },
					"numEmps", 4L)).
			addExpected(
				createJsonObject("dept", 2L, "deptName", "marketing", "emps", new long[] { 3L, 6L }, "numEmps", 2L)).
			addExpected(createJsonObject("dept", 3L, "deptName", "sales", "emps", new long[] { 5L }, "numEmps", 1L));

		sopremoPlan.run();
	}

	@Test
	public void shouldGroupWithSingleSource() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment("d", createPath("$", "dept")));
		transformation.addMapping(new ValueAssignment("total", new Function("sum", createPath("$", "[*]", "income"))));
		sopremoPlan.getOutputOperator(0).setInputOperators(
			new Aggregation(transformation, Arrays.asList(createPath("$", "dept")), sopremoPlan.getInputOperator(0)));

		sopremoPlan.getInput(0).
			add(createJsonObject("id", 1L, "dept", 1L, "income", 12000L)).
			add(createJsonObject("id", 2L, "dept", 1L, "income", 13000L)).
			add(createJsonObject("id", 3L, "dept", 2L, "income", 15000L)).
			add(createJsonObject("id", 4L, "dept", 1L, "income", 10000L)).
			add(createJsonObject("id", 5L, "dept", 3L, "income", 8000L)).
			add(createJsonObject("id", 6L, "dept", 2L, "income", 5000L)).
			add(createJsonObject("id", 7L, "dept", 1L, "income", 24000L));
		sopremoPlan.getOutput(0).
			addExpected(createJsonObject("d", 1L, "total", 59000L)).
			addExpected(createJsonObject("d", 2L, "total", 20000L)).
			addExpected(createJsonObject("d", 3L, "total", 8000L));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformSimpleGroupBy() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment(new Function("count", new Input(0))));
		sopremoPlan.getOutputOperator(0).setInputOperators(
			new Aggregation(transformation, Aggregation.NO_GROUPING, sopremoPlan.getInputOperator(0)));

		sopremoPlan.getInput(0).
			add(createJsonObject("id", 1L, "dept", 1L, "income", 12000L)).
			add(createJsonObject("id", 2L, "dept", 1L, "income", 13000L)).
			add(createJsonObject("id", 3L, "dept", 2L, "income", 15000L)).
			add(createJsonObject("id", 4L, "dept", 1L, "income", 10000L)).
			add(createJsonObject("id", 5L, "dept", 3L, "income", 8000L)).
			add(createJsonObject("id", 6L, "dept", 2L, "income", 5000L)).
			add(createJsonObject("id", 7L, "dept", 1L, "income", 24000L));
		sopremoPlan.getOutput(0).
			addExpected(createJsonValue(7L));

		sopremoPlan.run();
	}
}
