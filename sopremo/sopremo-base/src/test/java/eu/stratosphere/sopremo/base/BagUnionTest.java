package eu.stratosphere.sopremo.base;

import static eu.stratosphere.sopremo.JsonUtil.createValueNode;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class BagUnionTest extends SopremoTest<UnionAll> {
	@Override
	public void shouldComplyEqualsContract() {
	}

	@Test
	public void shouldPerformThreeWayBagUnion() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(3, 1);

		final UnionAll union = new UnionAll(sopremoPlan.getInputOperators(0, 3));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2));
		sopremoPlan.getInput(1).
			add(createValueNode((Object) 3)).
			add(createValueNode((Object) 4)).
			add(createValueNode((Object) 5));
		sopremoPlan.getInput(2).
			add(createValueNode((Object) 6)).
			add(createValueNode((Object) 7)).
			add(createValueNode((Object) 8));
		sopremoPlan.getExpectedOutput(0).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2)).
			add(createValueNode((Object) 3)).
			add(createValueNode((Object) 4)).
			add(createValueNode((Object) 5)).
			add(createValueNode((Object) 6)).
			add(createValueNode((Object) 7)).
			add(createValueNode((Object) 8));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformTrivialBagUnion() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		final UnionAll union = new UnionAll(sopremoPlan.getInputOperators(0, 1));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2));
		sopremoPlan.getExpectedOutput(0).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformTwoWayBagUnion() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final UnionAll union = new UnionAll(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2));
		sopremoPlan.getInput(1).
			add(createValueNode((Object) 3)).
			add(createValueNode((Object) 4)).
			add(createValueNode((Object) 5));
		sopremoPlan.getExpectedOutput(0).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2)).
			add(createValueNode((Object) 3)).
			add(createValueNode((Object) 4)).
			add(createValueNode((Object) 5));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformTwoWayBagUnionWithBagSemanticsPerDefault() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final UnionAll union = new UnionAll(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(union);

		sopremoPlan.getInput(0).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2));
		sopremoPlan.getInput(1).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2)).
			add(createValueNode((Object) 3));
		sopremoPlan.getExpectedOutput(0).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2)).
			add(createValueNode((Object) 3)).
			add(createValueNode((Object) 1)).
			add(createValueNode((Object) 2));

		sopremoPlan.run();
	}

}
