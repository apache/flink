package eu.stratosphere.meteor.base;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorTest;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.SopremoPlan;

public class UnionAllTest extends MeteorTest {
	@Test
	public void testUnionAll1() {
		final SopremoPlan actualPlan = this.parseScript("$users1 = read from 'users1.json';\n" +
			"$users2 = read from 'users2.json';\n" +
			"$allUsers = union all $users1, $users2;\n" +
			"write $allUsers to 'allUsers.json';");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source users1 = new Source("users1.json");
		final Source users2 = new Source("users2.json");
		final UnionAll union = new UnionAll().
			withInputs(users1, users2);
		final Sink output = new Sink("allUsers.json").withInputs(union);
		expectedPlan.setSinks(output);

		assertPlanEquals(expectedPlan, actualPlan);
	}
}
