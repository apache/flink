package eu.stratosphere.simple.jaql.base;

import org.junit.Test;

import eu.stratosphere.simple.jaql.SimpleTest;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.Join;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ObjectCreation;

public class JoinTest extends SimpleTest {

	@Test
	public void testJoin1() {
		SopremoPlan actualPlan = parseScript("$users = read 'users.json';" +
			"$pages = read 'pages.json';" +
			"$result = join $users, $pages" +
			"  where $users.id == $pages.userid" +
			"  into { $users.name, $pages.* };" +
			"write $result to 'result.json';");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source users = new Source("users.json");
		Source pages = new Source("pages.json");
		Join join = new Join().
			withInputs(users, pages).
			withJoinCondition(new ComparativeExpression(JsonUtil.createPath("0", "id"),
				BinaryOperator.EQUAL, JsonUtil.createPath("1", "userid"))).
			withResultProjection(new ObjectCreation(
				new ObjectCreation.FieldAssignment("name", JsonUtil.createPath("0", "name")),
				new ObjectCreation.CopyFields(JsonUtil.createPath("1"))
				));
		Sink result = new Sink("result.json").withInputs(join);
		expectedPlan.setSinks(result);

		assertEquals(expectedPlan, actualPlan);
	}
	
	@Test
	public void testJoin2() {
		SopremoPlan actualPlan = parseScript("$users = read 'users.json';" +
				"$pages = read 'pages.json';" +
				"$result = join $u in $users, $p in $pages" +
				"  where $u.id == $p.userid" +
				"  into { $u.name, $p.* };" +
				"write $result to 'result.json';");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source users = new Source("users.json");
		Source pages = new Source("pages.json");
		Join join = new Join().
			withInputs(users, pages).
			withJoinCondition(new ComparativeExpression(JsonUtil.createPath("0", "id"),
				BinaryOperator.EQUAL, JsonUtil.createPath("1", "userid"))).
			withResultProjection(new ObjectCreation(
				new ObjectCreation.FieldAssignment("name", JsonUtil.createPath("0", "name")),
				new ObjectCreation.CopyFields(JsonUtil.createPath("1"))
				));
		Sink result = new Sink("result.json").withInputs(join);
		expectedPlan.setSinks(result);

		assertEquals(expectedPlan, actualPlan);
	}
	
	@Test
	public void testJoin3() {
		SopremoPlan actualPlan = parseScript("$users = read 'users.json';" +
				"$pages = read 'pages.json';" +
				"$result = join preserve $u in $users, $p in $pages" +
				"  where $u.id == $p.userid" +
				"  into { $u.name, $p.* };" +
				"write $result to 'result.json';");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source users = new Source("users.json");
		Source pages = new Source("pages.json");
		Join join = new Join().
			withInputs(users, pages).
			withJoinCondition(new ComparativeExpression(JsonUtil.createPath("0", "id"),
				BinaryOperator.EQUAL, JsonUtil.createPath("1", "userid"))).
			withResultProjection(new ObjectCreation(
				new ObjectCreation.FieldAssignment("name", JsonUtil.createPath("0", "name")),
				new ObjectCreation.CopyFields(JsonUtil.createPath("1"))
				));
		Sink result = new Sink("result.json").withInputs(join);
		expectedPlan.setSinks(result);

		assertEquals(expectedPlan, actualPlan);
	}
}
