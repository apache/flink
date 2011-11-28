package eu.stratosphere.simple.jaql.base;

import org.junit.Test;

import eu.stratosphere.simple.jaql.SimpleTest;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.Intersection;

public class IntersectionTest extends SimpleTest {

	@Test
	public void testIntersection1() {
		final SopremoPlan actualPlan = this.parseScript("$users1 = read 'users1.json';\n" +
			"$users2 = read 'users2.json';\n" +
			"$commonUsers = intersect $users1, $users2;\n" +
			"write $commonUsers to 'commonUsers.json';");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source users1 = new Source("users1.json");
		final Source users2 = new Source("users2.json");
		final Intersection intersection = new Intersection().withInputs(users1, users2);
		final Sink output = new Sink("newUsers.json").withInputs(intersection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testIntersection2() {
		final SopremoPlan actualPlan = this.parseScript("$users1 = read 'users1.json';\n" +
			"$users2 = read 'users2.json';\n" +
			"$commonUsers = intersect $users1 on $users1.id, $users2 on $users2.id;\n" +
			"write $commonUsers to 'commonUsers.json';");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source users1 = new Source("users1.json");
		final Source users2 = new Source("users2.json");
		final Intersection intersection = new Intersection().
			withInputs(users1, users2).
			withIdentityKey(0, JsonUtil.createPath("0", "id")).
			withIdentityKey(1, JsonUtil.createPath("1", "id"));
		final Sink output = new Sink("newUsers.json").withInputs(intersection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

}
