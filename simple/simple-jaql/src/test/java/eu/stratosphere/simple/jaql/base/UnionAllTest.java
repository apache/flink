package eu.stratosphere.simple.jaql.base;

import org.junit.Test;

import eu.stratosphere.simple.jaql.SimpleTest;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.UnionAll;

public class UnionAllTest extends SimpleTest {
	@Test
	public void testUnionAll1() {
		SopremoPlan actualPlan = parseScript("$users1 = read 'users1.json';\n" +
			"$users2 = read 'users2.json';\n" +
			"$allUsers = union all $users1, $users2;\n" +
			"write $allUsers to 'allUsers.json';");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source users1 = new Source("users1.json");
		Source users2 = new Source("users2.json");
		UnionAll union = new UnionAll().
			withInputs(users1, users2);
		Sink output = new Sink("allUsers.json").withInputs(union);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}
}
