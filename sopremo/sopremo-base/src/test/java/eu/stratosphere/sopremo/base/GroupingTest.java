package eu.stratosphere.sopremo.base;
import static eu.stratosphere.sopremo.JsonUtil.createPath;
import static eu.stratosphere.sopremo.JsonUtil.createValueNode;

import org.junit.Test;

import eu.stratosphere.sopremo.BuiltinFunctions;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.expressions.AggregationExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ArrayProjection;
import eu.stratosphere.sopremo.expressions.BatchAggregationExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class GroupingTest extends SopremoTest<Grouping> {
	@Override
	protected Grouping createDefaultInstance(final int index) {
		final Grouping aggregation = new Grouping(new ConstantExpression(index), null, null);
		return aggregation;
	}

	@Test
	public void shouldGroupThreeSources() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(3, 1);
		sopremoPlan.getEvaluationContext().getFunctionRegistry().register(BuiltinFunctions.class);

		final BatchAggregationExpression batch = new BatchAggregationExpression();

		final ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("dept",
			new PathExpression(new InputSelection(0), batch.add(BuiltinFunctions.FIRST), new ObjectAccess("dept")));
		transformation.addMapping("deptName", createPath("1", "[0]", "name"));
		transformation.addMapping("emps",
			new PathExpression(new InputSelection(0), batch.add(BuiltinFunctions.SORT, new ObjectAccess("id"))));
		transformation.addMapping("numEmps",
			new PathExpression(new InputSelection(0), batch.add(BuiltinFunctions.COUNT)));
		transformation.addMapping("expenses",
			new PathExpression(new InputSelection(2),
				new ArrayProjection(new ArithmeticExpression(new ObjectAccess("costPerItem"),
					ArithmeticOperator.MULTIPLICATION, new ObjectAccess("count"))),
				new AggregationExpression(BuiltinFunctions.SUM)));

		final Grouping aggregation = new Grouping(transformation, sopremoPlan.getInputOperators(0, 3));
		aggregation.setKeyProjection(0, createPath("dept"));
		aggregation.setKeyProjection(1, createPath("did"));
		aggregation.setKeyProjection(2, createPath("dept_id"));

		sopremoPlan.getOutputOperator(0).setInputs(aggregation);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("id", 1, "dept", 1, "income", 12000)).
			add(createPactJsonObject("id", 2, "dept", 1, "income", 13000)).
			add(createPactJsonObject("id", 3, "dept", 2, "income", 15000)).
			add(createPactJsonObject("id", 4, "dept", 1, "income", 10000)).
			add(createPactJsonObject("id", 5, "dept", 3, "income", 8000)).
			add(createPactJsonObject("id", 6, "dept", 2, "income", 5000)).
			add(createPactJsonObject("id", 7, "dept", 1, "income", 24000));
		sopremoPlan.getInput(1).
			add(createPactJsonObject("did", 1, "name", "development")).
			add(createPactJsonObject("did", 2, "name", "marketing")).
			add(createPactJsonObject("did", 3, "name", "sales"));
		sopremoPlan.getInput(2).
			add(createPactJsonObject("item", "copy paper", "count", 100, "costPerItem", 1, "dept_id", 1)).
			add(createPactJsonObject("item", "copy paper", "count", 10000, "costPerItem", 2, "dept_id", 2)).
			add(createPactJsonObject("item", "copy paper", "count", 1000, "costPerItem", 1, "dept_id", 3)).
			add(createPactJsonObject("item", "poster", "count", 100, "costPerItem", 500, "dept_id", 2)).
			add(createPactJsonObject("item", "poster", "count", 10, "costPerItem", 300, "dept_id", 3));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonObject("dept", 1, "deptName", "development",
				"emps", new int[] { 1, 2, 4, 7 }, "numEmps", 4, "expenses", 100)).
			add(createPactJsonObject("dept", 2, "deptName", "marketing",
				"emps", new int[] { 3, 6 }, "numEmps", 2, "expenses", 20000 + 50000)).
			add(createPactJsonObject("dept", 3, "deptName", "sales",
				"emps", new int[] { 5 }, "numEmps", 1, "expenses", 1000 + 3000));

		sopremoPlan.run();
	}

	@Test
	public void shouldGroupTwoSources() {

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);
		sopremoPlan.getEvaluationContext().getFunctionRegistry().register(BuiltinFunctions.class);

		final BatchAggregationExpression batch = new BatchAggregationExpression();
		final ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("dept",
			new PathExpression(new InputSelection(0), batch.add(BuiltinFunctions.FIRST), new ObjectAccess("dept")));
		transformation.addMapping("deptName", createPath("1", "[0]", "name"));
		transformation.addMapping("emps",
			new PathExpression(new InputSelection(0), batch.add(BuiltinFunctions.SORT,
				new ObjectAccess("id"))));
		transformation.addMapping("numEmps",
			new PathExpression(new InputSelection(0), batch.add(BuiltinFunctions.COUNT)));

		final Grouping aggregation = new Grouping(transformation, sopremoPlan.getInputOperators(0, 2));
		aggregation.setKeyProjection(0, createPath("dept"));
		aggregation.setKeyProjection(1, createPath("did"));

		sopremoPlan.getOutputOperator(0).setInputs(aggregation);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("id", 1, "dept", 1, "income", 12000)).
			add(createPactJsonObject("id", 2, "dept", 1, "income", 13000)).
			add(createPactJsonObject("id", 3, "dept", 2, "income", 15000)).
			add(createPactJsonObject("id", 4, "dept", 1, "income", 10000)).
			add(createPactJsonObject("id", 5, "dept", 3, "income", 8000)).
			add(createPactJsonObject("id", 6, "dept", 2, "income", 5000)).
			add(createPactJsonObject("id", 7, "dept", 1, "income", 24000));
		sopremoPlan.getInput(1).
			add(createPactJsonObject("did", 1, "name", "development")).
			add(createPactJsonObject("did", 2, "name", "marketing")).
			add(createPactJsonObject("did", 3, "name", "sales"));
		sopremoPlan.getExpectedOutput(0).
			add(
				createPactJsonObject("dept", 1, "deptName", "development", "emps", new int[] { 1, 2, 4, 7 },
					"numEmps", 4)).
			add(
				createPactJsonObject("dept", 2, "deptName", "marketing", "emps", new int[] { 3, 6 }, "numEmps", 2)).
			add(createPactJsonObject("dept", 3, "deptName", "sales", "emps", new int[] { 5 }, "numEmps", 1));

		sopremoPlan.run();
	}

	@Test
	public void shouldGroupWithSingleSource() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);
		sopremoPlan.getEvaluationContext().getFunctionRegistry().register(BuiltinFunctions.class);

		final ObjectCreation transformation = new ObjectCreation();
		final BatchAggregationExpression batch = new BatchAggregationExpression();
		transformation.addMapping("d",
			new PathExpression(batch.add(BuiltinFunctions.FIRST), new ObjectAccess("dept")));
		transformation.addMapping("total",
			new PathExpression(batch.add(BuiltinFunctions.SUM, new ObjectAccess("income"))));

		final Grouping aggregation = new Grouping(transformation, sopremoPlan.getInputOperator(0));
		aggregation.setKeyProjection(0, createPath("dept"));

		sopremoPlan.getOutputOperator(0).setInputs(aggregation);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("id", 1, "dept", 1, "income", 12000)).
			add(createPactJsonObject("id", 2, "dept", 1, "income", 13000)).
			add(createPactJsonObject("id", 3, "dept", 2, "income", 15000)).
			add(createPactJsonObject("id", 4, "dept", 1, "income", 10000)).
			add(createPactJsonObject("id", 5, "dept", 3, "income", 8000)).
			add(createPactJsonObject("id", 6, "dept", 2, "income", 5000)).
			add(createPactJsonObject("id", 7, "dept", 1, "income", 24000));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonObject("d", 1, "total", 59000)).
			add(createPactJsonObject("d", 2, "total", 20000)).
			add(createPactJsonObject("d", 3, "total", 8000));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformSimpleGroupBy() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);
		sopremoPlan.getEvaluationContext().getFunctionRegistry().register(BuiltinFunctions.class);

		final Grouping aggregation = new Grouping(BuiltinFunctions.COUNT.asExpression(),
			sopremoPlan.getInputOperator(0));
		sopremoPlan.getOutputOperator(0).setInputs(aggregation);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("id", 1, "dept", 1, "income", 12000)).
			add(createPactJsonObject("id", 2, "dept", 1, "income", 13000)).
			add(createPactJsonObject("id", 3, "dept", 2, "income", 15000)).
			add(createPactJsonObject("id", 4, "dept", 1, "income", 10000)).
			add(createPactJsonObject("id", 5, "dept", 3, "income", 8000)).
			add(createPactJsonObject("id", 6, "dept", 2, "income", 5000)).
			add(createPactJsonObject("id", 7, "dept", 1, "income", 24000));
		sopremoPlan.getExpectedOutput(0).
			add(createValueNode((Object) 7));

		sopremoPlan.run();
	}
}
