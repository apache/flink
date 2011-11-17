package eu.stratosphere.simple.jaql.base;

import org.junit.Test;

import eu.stratosphere.simple.jaql.SimpleTest;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.Union;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.MethodCall;

public class UnionTest extends SimpleTest {

	@Test
	public void testUnion1() {
		SopremoPlan actualPlan = parseScript("$users1 = read 'users1.json';" +
			"$users2 = read 'users2.json';" +
			"$allUsers = union $users1, $users2;" +
			"write $allUsers to 'allUsers.json';");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source users1 = new Source("users1.json");
		Source users2 = new Source("users2.json");
		Union union = new Union().
			withInputs(users1, users2);
		Sink output = new Sink("allUsers.json").withInputs(union);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testUnion2() {
		SopremoPlan actualPlan = parseScript("$users1 = read 'users1.json';" +
			"$users2 = read 'users2.json';" +
			"$allUsers = union $users1 on $users1.firstName + $users1.lastName, $users2 on $users2.name;" +
			"write $allUsers to 'allUsers.json';");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source users1 = new Source("users1.json");
		Source users2 = new Source("users2.json");
		Union union = new Union().
			withInputs(users1, users2).
			withIdentityKey(0, new ArithmeticExpression(JsonUtil.createPath("0", "firstName"),
				ArithmeticOperator.ADDITION, JsonUtil.createPath("0", "lastName"))).
			withIdentityKey(1, JsonUtil.createPath("1", "name"));
		Sink output = new Sink("allUsers.json").withInputs(union);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testUnion3() {
		SopremoPlan actualPlan = parseScript("$users1 = read 'users1.json';" +
			"$users2 = read 'users2.json';" +
			"$allUsers = union $users1 on concat($users1.firstName, $users1.lastName), $users2 on $users2.name;" +
			"write $allUsers to 'allUsers.json';");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source users1 = new Source("users1.json");
		Source users2 = new Source("users2.json");
		Union union = new Union().
			withInputs(users1, users2).
			withIdentityKey(0,
				new MethodCall("concat", JsonUtil.createPath("0", "firstName"),
					JsonUtil.createPath("0", "lastName"))).
			withIdentityKey(1, JsonUtil.createPath("1", "name"));
		Sink output = new Sink("allUsers.json").withInputs(union);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

}
