package eu.stratosphere.simple.jaql.base;

import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.simple.jaql.SimpleTest;
import eu.stratosphere.sopremo.ExpressionTag;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.Join;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;

public class JoinTest extends SimpleTest {

	@Test
	public void testJoin1() {
		SopremoPlan actualPlan = parseScript("$users = read 'users.json';\n" +
			"$pages = read 'pages.json';\n" +
			"$result = join $users, $pages\n" +
			"  where $users.id == $pages.userid\n" +
			"  into { $users.name, $pages.* };\n" +
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
		SopremoPlan actualPlan = parseScript("$users = read 'users.json';\n" +
			"$pages = read 'pages.json';\n" +
			"$result = join $u in $users, $p in $pages\n" +
			"  where $u.id == $p.userid\n" +
			"  into { $u.name, $p.* };\n" +
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
	public void testOuterJoin() {
		SopremoPlan actualPlan = parseScript("$users = read 'users.json';\n" +
			"$pages = read 'pages.json';\n" +
			"$result = join preserve $u in $users, $p in $pages\n" +
			"  where $u.id == $p.userid\n" +
			"  into { $u.name, $p.* };\n" +
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

		Iterator<? extends Operator<?>> iterator = actualPlan.getContainedOperators().iterator();
		Join actualJoin = null;
		while (iterator.hasNext()) {
			Operator<?> operator = (Operator<?>) iterator.next();
			if (operator instanceof Join)
				actualJoin = (Join) operator;
		}
		Assert.assertTrue(((ComparativeExpression) actualJoin.getJoinCondition()).getExpr1().
			find(InputSelection.class).hasTag(ExpressionTag.RETAIN));
	}
}
