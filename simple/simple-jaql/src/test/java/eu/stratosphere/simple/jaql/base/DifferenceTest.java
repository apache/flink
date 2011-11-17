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
		SopremoPlan actualPlan = parseScript("$oldUsers = read 'oldUsers.json';" +
			"$currentUsers = read 'currentUsers.json';" +
			"$newUsers = difference $currentUsers, $oldUsers;" +
			"write $newUsers to 'newUsers.json';");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source old = new Source("oldUsers.json");
		Source current = new Source("currentUsers.json");
		Difference difference = new Difference().
			withInputs(current, old);
		Sink output = new Sink("newUsers.json").
			withInputs(difference);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testDifference2() {
		SopremoPlan actualPlan = parseScript("$oldUsers = read 'oldUsers.json';" +
			"$currentUsers = read 'currentUsers.json';" +
			"$newUsers = difference $currentUsers on $currentUsers.id, $oldUsers on $oldUsers.id;" +
			"write $newUsers to 'newUsers.json';");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source old = new Source("oldUsers.json");
		Source current = new Source("currentUsers.json");
		Difference difference = new Difference().
			withInputs(current, old).
			withIdentityKey(0, JsonUtil.createPath("0", "id")).
			withIdentityKey(1, JsonUtil.createPath("1", "id"));
		Sink output = new Sink("newUsers.json").
			withInputs(difference);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

}
