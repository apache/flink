package eu.stratosphere.sopremo.base;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class BagUnionTest extends SopremoTest<UnionAll> {
	@Override
	public void shouldComplyEqualsContract() {
	}

	@Test
	public void shouldPerformTrivialBagUnion() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		UnionAll union = new UnionAll(sopremoPlan.getInputOperators(0, 1));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformTwoWayBagUnion() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		UnionAll union = new UnionAll(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2));
		sopremoPlan.getInput(1).
			add(createPactJsonValue(3)).
			add(createPactJsonValue(4)).
			add(createPactJsonValue(5));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2)).
			add(createPactJsonValue(3)).
			add(createPactJsonValue(4)).
			add(createPactJsonValue(5));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformThreeWayBagUnion() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(3, 1);

		UnionAll union = new UnionAll(sopremoPlan.getInputOperators(0, 3));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2));
		sopremoPlan.getInput(1).
			add(createPactJsonValue(3)).
			add(createPactJsonValue(4)).
			add(createPactJsonValue(5));
		sopremoPlan.getInput(2).
			add(createPactJsonValue(6)).
			add(createPactJsonValue(7)).
			add(createPactJsonValue(8));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2)).
			add(createPactJsonValue(3)).
			add(createPactJsonValue(4)).
			add(createPactJsonValue(5)).
			add(createPactJsonValue(6)).
			add(createPactJsonValue(7)).
			add(createPactJsonValue(8));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformTwoWayBagUnionWithBagSemanticsPerDefault() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		UnionAll union = new UnionAll(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2));
		sopremoPlan.getInput(1).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2)).
			add(createPactJsonValue(3));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2)).
			add(createPactJsonValue(3)).
			add(createPactJsonValue(1)).
			add(createPactJsonValue(2));

		sopremoPlan.run();
	}

}
