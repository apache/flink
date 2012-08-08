package eu.stratosphere.sopremo.base;
import static eu.stratosphere.sopremo.type.JsonUtil.createPath;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.expressions.AndExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.OrExpression;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class SelectionTest extends SopremoTest<Selection> {
	@Override
	protected Selection createDefaultInstance(final int index) {
		final AndExpression condition = new AndExpression(new UnaryExpression(
			createPath(String.valueOf(index))));
		return new Selection().withCondition(condition);
	}

	@Test
	public void shouldSelectSomeEntries() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		final ComparativeExpression incomeComparison = new ComparativeExpression(new ObjectAccess("income"),
			BinaryOperator.GREATER, new ConstantExpression(30000));
		final UnaryExpression mgrFlag = new UnaryExpression(new ObjectAccess("mgr"));
		final OrExpression condition = new OrExpression(mgrFlag, incomeComparison);
		sopremoPlan.getOutputOperator(0).setInputs(
			new Selection().
				withCondition(condition).
				withInputs(sopremoPlan.getInputOperator(0)));
		sopremoPlan.getInput(0).
			addObject("name", "Jon Doe", "income", 20000, "mgr", false).
			addObject("name", "Vince Wayne", "income", 32500, "mgr", false).
			addObject("name", "Jane Dean", "income", 72000, "mgr", true).
			addObject("name", "Alex Smith", "income", 25000, "mgr", false);
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Vince Wayne", "income", 32500, "mgr", false).
			addObject("name", "Jane Dean", "income", 72000, "mgr", true);

		sopremoPlan.run();
	}
	
	@Test
	public void shouldSelectSomeEntriesWithInputSelection() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		final ComparativeExpression incomeComparison = new ComparativeExpression(createPath("0", "income"),
			BinaryOperator.GREATER, new ConstantExpression(30000));
		final UnaryExpression mgrFlag = new UnaryExpression(createPath("0", "mgr"));
		final OrExpression condition = new OrExpression(mgrFlag, incomeComparison);
		sopremoPlan.getOutputOperator(0).setInputs(
			new Selection().
				withCondition(condition).
				withInputs(sopremoPlan.getInputOperator(0)));
		sopremoPlan.getInput(0).
			addObject("name", "Jon Doe", "income", 20000, "mgr", false).
			addObject("name", "Vince Wayne", "income", 32500, "mgr", false).
			addObject("name", "Jane Dean", "income", 72000, "mgr", true).
			addObject("name", "Alex Smith", "income", 25000, "mgr", false);
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Vince Wayne", "income", 32500, "mgr", false).
			addObject("name", "Jane Dean", "income", 72000, "mgr", true);

		sopremoPlan.run();
	}
}
