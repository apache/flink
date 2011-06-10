package eu.stratosphere.sopremo.operator;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.SopremoTestPlan;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.expressions.Comparison;
import eu.stratosphere.sopremo.expressions.Condition;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.expressions.Comparison.BinaryOperator;
import eu.stratosphere.sopremo.expressions.Condition.Combination;
import eu.stratosphere.sopremo.expressions.Constant;

public class SelectionTest extends SopremoTest<Selection> {
	@Override
	protected Selection createDefaultInstance(int index) {
		Condition condition = new Condition(new UnaryExpression(createPath(String.valueOf(index))));
		return new Selection(condition, null);
	}

	@Test
	public void shouldSelectSomeEntries() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		Comparison incomeComparison = new Comparison(createPath("income"), BinaryOperator.GREATER, new Constant(
			30000));
		UnaryExpression mgrFlag = new UnaryExpression(createPath("mgr"));
		Condition condition = new Condition(Combination.OR, mgrFlag, incomeComparison);
		sopremoPlan.getOutputOperator(0).setInputs(
			new Selection(condition, sopremoPlan.getInputOperator(0)));

		sopremoPlan.getInput(0).
			add(createPactJsonObject("name", "Jon Doe", "income", 20000, "mgr", false)).
			add(createPactJsonObject("name", "Vince Wayne", "income", 32500, "mgr", false)).
			add(createPactJsonObject("name", "Jane Dean", "income", 72000, "mgr", true)).
			add(createPactJsonObject("name", "Alex Smith", "income", 25000, "mgr", false));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonObject("name", "Vince Wayne", "income", 32500, "mgr", false)).
			add(createPactJsonObject("name", "Jane Dean", "income", 72000, "mgr", true));

		sopremoPlan.run();
	}
}
