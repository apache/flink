package eu.stratosphere.sopremo.operator;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.SopremoTestPlan;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ConditionalExpression;
import eu.stratosphere.sopremo.expressions.FieldAccess;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConditionalExpression.Combination;
import eu.stratosphere.sopremo.expressions.ConstantExpression;

public class SelectionTest extends SopremoTest<Selection> {
	@Override
	protected Selection createDefaultInstance(int index) {
		ConditionalExpression condition = new ConditionalExpression(new UnaryExpression(
			createPath(String.valueOf(index))));
		return new Selection(condition, null);
	}

	@Test
	public void shouldSelectSomeEntries() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		ComparativeExpression incomeComparison = new ComparativeExpression(new FieldAccess("income"),
			BinaryOperator.GREATER, new ConstantExpression(30000));
		UnaryExpression mgrFlag = new UnaryExpression(new FieldAccess("mgr"));
		ConditionalExpression condition = new ConditionalExpression(Combination.OR, mgrFlag, incomeComparison);
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
