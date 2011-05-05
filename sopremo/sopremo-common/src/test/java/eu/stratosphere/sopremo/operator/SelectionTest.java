package eu.stratosphere.sopremo.operator;

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
import eu.stratosphere.sopremo.expressions.Transformation;
import eu.stratosphere.sopremo.expressions.ValueAssignment;
import eu.stratosphere.sopremo.expressions.Arithmetic.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.Constant;

public class SelectionTest extends SopremoTest {
	@Test
	public void shouldSelectSomeEntries() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		Comparison incomeComparison = new Comparison(createPath("0", "income"), BinaryOperator.GREATER, new Constant(
			30000));
		UnaryExpression mgrFlag = new UnaryExpression(createPath("0", "mgr"));
		Condition condition = new Condition(Combination.OR, mgrFlag, incomeComparison);
		sopremoPlan.getOutputOperator(0).setInputOperators(
			new Selection(condition, sopremoPlan.getInputOperator(0)));

		sopremoPlan.getInput(0).
			add(createJsonObject("name", "Jon Doe", "income", 20000L, "mgr", false)).
			add(createJsonObject("name", "Vince Wayne", "income", 32500L, "mgr", false)).
			add(createJsonObject("name", "Jane Dean", "income", 72000L, "mgr", true)).
			add(createJsonObject("name", "Alex Smith", "income", 25000L, "mgr", false));
		sopremoPlan.getOutput(0).
			addExpected(createJsonObject("name", "Jon Doe", "income", 20000L, "mgr", false)).
			addExpected(createJsonObject("name", "Jane Dean", "income", 72000L, "mgr", true));

		sopremoPlan.run();
	}
}
