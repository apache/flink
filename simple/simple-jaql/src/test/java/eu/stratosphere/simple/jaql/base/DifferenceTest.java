package eu.stratosphere.simple.jaql.base;

import org.junit.Test;

import eu.stratosphere.simple.jaql.SimpleTest;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.Difference;

public class DifferenceTest extends SimpleTest {

	@Test
	public void testDifference1() {
		final SopremoPlan actualPlan = this.parseScript("$oldUsers = read 'oldUsers.json';\n" +
			"$currentUsers = read 'currentUsers.json';\n" +
			"$newUsers = difference $currentUsers, $oldUsers;\n" +
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

	@Test
	public void testDifference2() {
		final SopremoPlan actualPlan = this.parseScript("$oldUsers = read 'oldUsers.json';\n" +
			"$currentUsers = read 'currentUsers.json';\n" +
			"$newUsers = difference $currentUsers on $currentUsers.id, $oldUsers on $oldUsers.id;\n" +
			"write $newUsers to 'newUsers.json';");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source old = new Source("oldUsers.json");
		final Source current = new Source("currentUsers.json");
		final Difference difference = new Difference().
			withInputs(current, old).
			withIdentityKey(0, JsonUtil.createPath("0", "id")).
			withIdentityKey(1, JsonUtil.createPath("1", "id"));
		final Sink output = new Sink("newUsers.json").
			withInputs(difference);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

}
