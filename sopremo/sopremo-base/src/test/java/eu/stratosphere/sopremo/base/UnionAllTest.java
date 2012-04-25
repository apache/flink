package eu.stratosphere.sopremo.base;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class UnionAllTest extends SopremoTest<UnionAll> {
	@Test
	public void shouldPerformThreeWayBagUnion() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(3, 1);

		final UnionAll union = new UnionAll();
		union.setInputs(sopremoPlan.getInputOperators(0, 3));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			addValue(1).
			addValue(2);
		sopremoPlan.getInput(1).
			addValue(3).
			addValue(4).
			addValue(5);
		sopremoPlan.getInput(2).
			addValue(6).
			addValue(7).
			addValue(8);
		sopremoPlan.getExpectedOutput(0).
			addValue(1).
			addValue(2).
			addValue(3).
			addValue(4).
			addValue(5).
			addValue(6).
			addValue(7).
			addValue(8);

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformTrivialBagUnion() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		final UnionAll union = new UnionAll();
		union.setInputs(sopremoPlan.getInputOperator(0));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			addValue(1).
			addValue(2);
		sopremoPlan.getExpectedOutput(0).
			addValue(1).
			addValue(2);

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformTwoWayBagUnion() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final UnionAll union = new UnionAll();
		union.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			addValue(1).
			addValue(2);
		sopremoPlan.getInput(1).
			addValue(3).
			addValue(4).
			addValue(5);
		sopremoPlan.getExpectedOutput(0).
			addValue(1).
			addValue(2).
			addValue(3).
			addValue(4).
			addValue(5);

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformTwoWayBagUnionWithBagSemanticsPerDefault() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final UnionAll union = new UnionAll();
		union.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			addValue(1).
			addValue(2);
		sopremoPlan.getInput(1).
			addValue(1).
			addValue(2).
			addValue(3);
		sopremoPlan.getExpectedOutput(0).
			addValue(1).
			addValue(2).
			addValue(3).
			addValue(1).
			addValue(2);

		sopremoPlan.run();
	}

}
