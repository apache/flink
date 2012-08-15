package eu.stratosphere.meteor.base;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorTest;
import eu.stratosphere.sopremo.base.Difference;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.SopremoPlan;

public class DifferenceTest extends MeteorTest {

	@Test
	public void testDifference() {
		final SopremoPlan actualPlan = this.parseScript("$oldUsers = read from 'oldUsers.json';\n" +
			"$currentUsers = read from 'currentUsers.json';\n" +
			"$newUsers = subtract $currentUsers, $oldUsers;\n" +
			"write $newUsers to 'newUsers.json';");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source old = new Source("oldUsers.json");
		final Source current = new Source("currentUsers.json");
		final Difference difference = new Difference().
			withInputs(current, old);
		final Sink output = new Sink("newUsers.json").
			withInputs(difference);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

}
