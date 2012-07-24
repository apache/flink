package eu.stratosphere.sopremo.base;

import static eu.stratosphere.sopremo.type.JsonUtil.createPath;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.expressions.AndExpression;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ExpressionTag;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression.Quantor;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class JoinTest extends SopremoTest<Join> {
	@Override
	protected Join createDefaultInstance(final int index) {
		final ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("field", createPath("0", "[" + index + "]"));
		final BooleanExpression condition = new ComparativeExpression(createPath("0", "id"),
			BinaryOperator.EQUAL, createPath("1", "userid"));
		return new Join().
			withJoinCondition(condition).
			withResultProjection(transformation);
	}

	@Test
	public void shouldPerformAntiJoin() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final AndExpression condition = new AndExpression(new ElementInSetExpression(
			createPath("0", "DeptName"), Quantor.EXISTS_NOT_IN, createPath("1", "Name")));
		final Join join = new Join().withJoinCondition(condition);
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(0).
			addObject("Name", "Harry", "EmpId", 3415, "DeptName", "Finance").
			addObject("Name", "Sally", "EmpId", 2241, "DeptName", "Sales").
			addObject("Name", "George", "EmpId", 3401, "DeptName", "Finance").
			addObject("Name", "Harriet", "EmpId", 2202, "DeptName", "Production");
		sopremoPlan.getInput(1).
			addObject("Name", "Sales", "Manager", "Harriet").
			addObject("Name", "Production", "Manager", "Charles");
		sopremoPlan.getExpectedOutput(0).
			addObject("Name", "Harry", "EmpId", 3415, "DeptName", "Finance").
			addObject("Name", "George", "EmpId", 3401, "DeptName", "Finance");
		
		sopremoPlan.run();
	}

	@Test
	public void shouldPerformEquiJoin() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final AndExpression condition = new AndExpression(new ComparativeExpression(createPath("0", "id"),
			BinaryOperator.EQUAL, createPath("1", "userid")));
		final Join join = new Join().withJoinCondition(condition);
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(1).
			addObject("userid", 1, "url", "code.google.com/p/jaql/").
			addObject("userid", 2, "url", "www.cnn.com").
			addObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/");
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url", "code.google.com/p/jaql/").
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
				"java.sun.com/javase/6/docs/api/").
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url", "www.cnn.com");

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformEquiJoinOnThreeInputs() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(3, 1);

		final AndExpression condition = new AndExpression(
			new ComparativeExpression(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1", "userid")),
			new ComparativeExpression(createPath("1", "url"), BinaryOperator.EQUAL, createPath("2", "page")));
		final ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("name", createPath("0", "name"));
		transformation.addMapping("url", createPath("1", "url"));
		transformation.addMapping("company", createPath("2", "company"));
		final Join join = new Join().withJoinCondition(condition).
			withResultProjection(transformation);
		join.setInputs(sopremoPlan.getInputOperators(0, 3));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(1).
			addObject("userid", 1, "url", "code.google.com/p/jaql/").
			addObject("userid", 2, "url", "www.oracle.com").
			addObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/").
			addObject("userid", 3, "url", "www.oracle.com");
		sopremoPlan.getInput(2).
			addObject("page", "code.google.com/p/jaql/", "company", "ibm").
			addObject("page", "www.oracle.com", "company", "oracle").
			addObject("page", "java.sun.com/javase/6/docs/api/", "company", "oracle");
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Jon Doe", "url", "code.google.com/p/jaql/", "company", "ibm").
			addObject("name", "Jon Doe", "url", "java.sun.com/javase/6/docs/api/", "company", "oracle").
			addObject("name", "Jane Doe", "url", "www.oracle.com", "company", "oracle").
			addObject("name", "Max Mustermann", "url", "www.oracle.com", "company", "oracle");
		
		sopremoPlan.run();
	}

	@Test
	public void shouldPerformFullOuterJoin() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		// here we set outer join flag
		final EvaluationExpression leftJoinKey = createPath("0", "id").withTag(ExpressionTag.RETAIN);
		final EvaluationExpression rightJoinKey = createPath("1", "userid").withTag(ExpressionTag.RETAIN);
		final AndExpression condition = new AndExpression(new ComparativeExpression(leftJoinKey,
			BinaryOperator.EQUAL, rightJoinKey));
		final Join join = new Join().withJoinCondition(condition);
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(1).
			addObject("userid", 1, "url", "code.google.com/p/jaql/").
			addObject("userid", 2, "url", "www.cnn.com").
			addObject("userid", 4, "url", "www.nbc.com").
			addObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/");
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url", "code.google.com/p/jaql/").
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
				"java.sun.com/javase/6/docs/api/").
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url", "www.cnn.com").
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3).
			addObject("userid", 4, "url", "www.nbc.com");

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformLeftOuterJoin() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		// here we set outer join flag
		final EvaluationExpression leftJoinKey = createPath("0", "id").withTag(ExpressionTag.RETAIN);
		final AndExpression condition = new AndExpression(new ComparativeExpression(leftJoinKey,
			BinaryOperator.EQUAL, createPath("1", "userid")));
		final Join join = new Join().withJoinCondition(condition);
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(1).
			addObject("userid", 1, "url", "code.google.com/p/jaql/").
			addObject("userid", 2, "url", "www.cnn.com").
			addObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/");
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url", "code.google.com/p/jaql/").
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
				"java.sun.com/javase/6/docs/api/").
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url", "www.cnn.com").
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformRightOuterJoin() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		// here we set outer join flag
		final EvaluationExpression rightJoinKey = createPath("1", "userid").withTag(ExpressionTag.RETAIN);
		final AndExpression condition = new AndExpression(new ComparativeExpression(createPath("0", "id"),
			BinaryOperator.EQUAL, rightJoinKey));
		final Join join = new Join().withJoinCondition(condition);
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(1).
			addObject("userid", 1, "url", "code.google.com/p/jaql/").
			addObject("userid", 2, "url", "www.cnn.com").
			addObject("userid", 4, "url", "www.nbc.com").
			addObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/");
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url", "code.google.com/p/jaql/").
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
				"java.sun.com/javase/6/docs/api/").
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url", "www.cnn.com").
			addObject("userid", 4, "url", "www.nbc.com");

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformSemiJoin() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final AndExpression condition = new AndExpression(new ElementInSetExpression(createPath("0",
			"DeptName"), Quantor.EXISTS_IN,
			createPath("1", "Name")));
		final Join join = new Join().withJoinCondition(condition);
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(0).
			addObject("Name", "Harry", "EmpId", 3415, "DeptName", "Finance").
			addObject("Name", "Sally", "EmpId", 2241, "DeptName", "Sales").
			addObject("Name", "George", "EmpId", 3401, "DeptName", "Finance").
			addObject("Name", "Harriet", "EmpId", 2202, "DeptName", "Production");
		sopremoPlan.getInput(1).
			addObject("Name", "Sales", "Manager", "Harriet").
			addObject("Name", "Production", "Manager", "Charles");
		sopremoPlan.getExpectedOutput(0).
			addObject("Name", "Sally", "EmpId", 2241, "DeptName", "Sales").
			addObject("Name", "Harriet", "EmpId", 2202, "DeptName", "Production");

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformThetaJoin() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		final AndExpression condition = new AndExpression(new ComparativeExpression(createPath("0", "id"),
			BinaryOperator.LESS, createPath("1",
				"userid")));
		final Join join = new Join().withJoinCondition(condition);
		join.setInputs(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);
		sopremoPlan.getInput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1).
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2).
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3);
		sopremoPlan.getInput(1).
			addObject("userid", 1, "url", "code.google.com/p/jaql/").
			addObject("userid", 2, "url", "www.cnn.com").
			addObject("userid", 4, "url", "www.nbc.com").
			addObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/");
		sopremoPlan.getExpectedOutput(0).
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 2, "url", "www.cnn.com").
			addObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 4, "url", "www.nbc.com").
			addObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 4, "url", "www.nbc.com").
			addObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3, "userid", 4, "url", "www.nbc.com");

		sopremoPlan.run();
	}
}
