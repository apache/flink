package eu.stratosphere.sopremo.operator;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.SopremoTestPlan;
import eu.stratosphere.sopremo.base.BagUnion;

public class BagUnionTest extends SopremoTest {

	@Test
	public void shouldPerformTrivialBagUnion() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(1, 1);

		BagUnion union = new BagUnion(sopremoPlan.getInputOperators(0, 1));
		sopremoPlan.getOutputOperator(0).setInputOperators(union);

		sopremoPlan.getInput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformTwoWayBagUnion() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		BagUnion union = new BagUnion(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputOperators(union);

		sopremoPlan.getInput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2));
		sopremoPlan.getInput(1).
			add(createJsonValue(3)).
			add(createJsonValue(4)).
			add(createJsonValue(5));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2)).
			add(createJsonValue(3)).
			add(createJsonValue(4)).
			add(createJsonValue(5));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformThreeWayBagUnion() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(3, 1);

		BagUnion union = new BagUnion(sopremoPlan.getInputOperators(0, 3));
		sopremoPlan.getOutputOperator(0).setInputOperators(union);

		sopremoPlan.getInput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2));
		sopremoPlan.getInput(1).
			add(createJsonValue(3)).
			add(createJsonValue(4)).
			add(createJsonValue(5));
		sopremoPlan.getInput(2).
			add(createJsonValue(6)).
			add(createJsonValue(7)).
			add(createJsonValue(8));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2)).
			add(createJsonValue(3)).
			add(createJsonValue(4)).
			add(createJsonValue(5)).
			add(createJsonValue(6)).
			add(createJsonValue(7)).
			add(createJsonValue(8));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformTwoWayBagUnionWithBagSemanticsPerDefault() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		BagUnion union = new BagUnion(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputOperators(union);

		sopremoPlan.getInput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2));
		sopremoPlan.getInput(1).
			add(createJsonValue(1)).
			add(createJsonValue(2)).
			add(createJsonValue(3));
		sopremoPlan.getExpectedOutput(0).
			add(createJsonValue(1)).
			add(createJsonValue(2)).
			add(createJsonValue(3)).
			add(createJsonValue(1)).
			add(createJsonValue(2));

		sopremoPlan.run();
	}

}
