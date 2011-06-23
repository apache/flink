package eu.stratosphere.sopremo.operator;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.SopremoTestPlan;
import eu.stratosphere.sopremo.base.Join;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ConditionalExpression;
import eu.stratosphere.sopremo.expressions.ConditionalExpression.Combination;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.ExpressionTag;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression.Quantor;
import eu.stratosphere.sopremo.expressions.PathExpression;

public class JoinTest extends SopremoTest<Join> {
	@Override
	protected Join createDefaultInstance(int index) {
		ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("field", createPath("0", "[" + index + "]"));
		ConditionalExpression condition = new ConditionalExpression(new ComparativeExpression(createPath("0", "id"),
			BinaryOperator.EQUAL, createPath("1",
				"userid")));
		return new Join(transformation, condition, null, null);
	}

	@Test
	public void shouldPerformEquiJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		ConditionalExpression condition = new ConditionalExpression(new ComparativeExpression(createPath("0", "id"),
			BinaryOperator.EQUAL, createPath("1", "userid")));
		Join join = new Join(ObjectCreation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createPactJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createPactJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));
		sopremoPlan.getInput(1).
			add(createPactJsonObject("userid", 1, "url", "code.google.com/p/jaql/")).
			add(createPactJsonObject("userid", 2, "url", "www.cnn.com")).
			add(createPactJsonObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/"));
		sopremoPlan.getExpectedOutput(0).
			add(
				createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"code.google.com/p/jaql/")).
			add(
				createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"java.sun.com/javase/6/docs/api/")).
			add(
				createPactJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url",
					"www.cnn.com"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformLeftOuterJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		// here we set outer join flag
		EvaluableExpression leftJoinKey = createPath("0", "id").withTag(ExpressionTag.PRESERVE);
		ConditionalExpression condition = new ConditionalExpression(new ComparativeExpression(leftJoinKey,
			BinaryOperator.EQUAL, createPath("1", "userid")));
		Join join = new Join(ObjectCreation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createPactJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createPactJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));
		sopremoPlan.getInput(1).
			add(createPactJsonObject("userid", 1, "url", "code.google.com/p/jaql/")).
			add(createPactJsonObject("userid", 2, "url", "www.cnn.com")).
			add(createPactJsonObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/"));
		sopremoPlan.getExpectedOutput(0).
			add(
				createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"code.google.com/p/jaql/")).
			add(
				createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"java.sun.com/javase/6/docs/api/")).
			add(
				createPactJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url",
					"www.cnn.com")).
			add(
				createPactJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformRightOuterJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		// here we set outer join flag
		EvaluableExpression rightJoinKey = createPath("1", "userid").withTag(ExpressionTag.PRESERVE);
		ConditionalExpression condition = new ConditionalExpression(new ComparativeExpression(createPath("0", "id"),
			BinaryOperator.EQUAL, rightJoinKey));
		Join join = new Join(ObjectCreation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createPactJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createPactJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));
		sopremoPlan.getInput(1).
			add(createPactJsonObject("userid", 1, "url", "code.google.com/p/jaql/")).
			add(createPactJsonObject("userid", 2, "url", "www.cnn.com")).
			add(createPactJsonObject("userid", 4, "url", "www.nbc.com")).
			add(createPactJsonObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/"));
		sopremoPlan.getExpectedOutput(0).
			add(
				createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"code.google.com/p/jaql/")).
			add(
				createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"java.sun.com/javase/6/docs/api/")).
			add(
				createPactJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url",
					"www.cnn.com")).
			add(
				createPactJsonObject("userid", 4, "url", "www.nbc.com"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformFullOuterJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		// here we set outer join flag
		EvaluableExpression leftJoinKey = createPath("0", "id").withTag(ExpressionTag.PRESERVE);
		EvaluableExpression rightJoinKey = createPath("1", "userid").withTag(ExpressionTag.PRESERVE);
		ConditionalExpression condition = new ConditionalExpression(new ComparativeExpression(leftJoinKey,
			BinaryOperator.EQUAL, rightJoinKey));
		Join join = new Join(ObjectCreation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createPactJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createPactJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));
		sopremoPlan.getInput(1).
			add(createPactJsonObject("userid", 1, "url", "code.google.com/p/jaql/")).
			add(createPactJsonObject("userid", 2, "url", "www.cnn.com")).
			add(createPactJsonObject("userid", 4, "url", "www.nbc.com")).
			add(createPactJsonObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/"));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"code.google.com/p/jaql/")).
			add(createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 1, "url",
					"java.sun.com/javase/6/docs/api/")).
			add(createPactJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 2, "url",
					"www.cnn.com")).
			add(createPactJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3)).
			add(createPactJsonObject("userid", 4, "url", "www.nbc.com"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformThetaJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		ConditionalExpression condition = new ConditionalExpression(new ComparativeExpression(createPath("0", "id"),
			BinaryOperator.LESS, createPath("1",
				"userid")));
		Join join = new Join(ObjectCreation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createPactJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createPactJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));
		sopremoPlan.getInput(1).
			add(createPactJsonObject("userid", 1, "url", "code.google.com/p/jaql/")).
			add(createPactJsonObject("userid", 2, "url", "www.cnn.com")).
			add(createPactJsonObject("userid", 4, "url", "www.nbc.com")).
			add(createPactJsonObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/"));
		sopremoPlan
			.getExpectedOutput(0)
			.
			add(
				createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 2, "url",
					"www.cnn.com"))
			.
			add(
				createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1, "userid", 4, "url",
					"www.nbc.com"))
			.
			add(
				createPactJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2, "userid", 4, "url",
					"www.nbc.com"))
			.
			add(
				createPactJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3, "userid", 4, "url",
					"www.nbc.com"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformAntiJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		ConditionalExpression condition = new ConditionalExpression(new ElementInSetExpression(
			createPath("0", "DeptName"), Quantor.EXISTS_NOT_IN, createPath("1", "Name")));
		Join join = new Join(ObjectCreation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("Name", "Harry", "EmpId", 3415, "DeptName", "Finance")).
			add(createPactJsonObject("Name", "Sally", "EmpId", 2241, "DeptName", "Sales")).
			add(createPactJsonObject("Name", "George", "EmpId", 3401, "DeptName", "Finance")).
			add(createPactJsonObject("Name", "Harriet", "EmpId", 2202, "DeptName", "Production"));
		sopremoPlan.getInput(1).
			add(createPactJsonObject("Name", "Sales", "Manager", "Harriet")).
			add(createPactJsonObject("Name", "Production", "Manager", "Charles"));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonObject("Name", "Harry", "EmpId", 3415, "DeptName", "Finance")).
			add(createPactJsonObject("Name", "George", "EmpId", 3401, "DeptName", "Finance"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformSemiJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		ConditionalExpression condition = new ConditionalExpression(new ElementInSetExpression(createPath("0",
			"DeptName"), Quantor.EXISTS_IN,
			createPath("1", "Name")));
		Join join = new Join(ObjectCreation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputs(join);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("Name", "Harry", "EmpId", 3415, "DeptName", "Finance")).
			add(createPactJsonObject("Name", "Sally", "EmpId", 2241, "DeptName", "Sales")).
			add(createPactJsonObject("Name", "George", "EmpId", 3401, "DeptName", "Finance")).
			add(createPactJsonObject("Name", "Harriet", "EmpId", 2202, "DeptName", "Production"));
		sopremoPlan.getInput(1).
			add(createPactJsonObject("Name", "Sales", "Manager", "Harriet")).
			add(createPactJsonObject("Name", "Production", "Manager", "Charles"));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonObject("Name", "Sally", "EmpId", 2241, "DeptName", "Sales")).
			add(createPactJsonObject("Name", "Harriet", "EmpId", 2202, "DeptName", "Production"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformEquiJoinOnThreeInputs() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(3, 1);

		ConditionalExpression condition = new ConditionalExpression(Combination.AND,
			new ComparativeExpression(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1", "userid")),
			new ComparativeExpression(createPath("1", "url"), BinaryOperator.EQUAL, createPath("2", "page")));
		ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("name", createPath("0", "name"));
		transformation.addMapping("url", createPath("1", "url"));
		transformation.addMapping("company", createPath("2", "company"));
		Join join = new Join(transformation, condition, sopremoPlan.getInputOperators(0, 3));
		sopremoPlan.getOutputOperator(0).setInputs(join);

		sopremoPlan.getInput(0).
			add(createPactJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1)).
			add(createPactJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2)).
			add(createPactJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3));
		sopremoPlan.getInput(1).
			add(createPactJsonObject("userid", 1, "url", "code.google.com/p/jaql/")).
			add(createPactJsonObject("userid", 2, "url", "www.oracle.com")).
			add(createPactJsonObject("userid", 1, "url", "java.sun.com/javase/6/docs/api/")).
			add(createPactJsonObject("userid", 3, "url", "www.oracle.com"));
		sopremoPlan.getInput(2).
			add(createPactJsonObject("page", "code.google.com/p/jaql/", "company", "ibm")).
			add(createPactJsonObject("page", "www.oracle.com", "company", "oracle")).
			add(createPactJsonObject("page", "java.sun.com/javase/6/docs/api/", "company", "oracle"));
		sopremoPlan.getExpectedOutput(0).
			add(createPactJsonObject("name", "Jon Doe", "url", "code.google.com/p/jaql/", "company", "ibm")).
			add(createPactJsonObject("name", "Jon Doe", "url", "java.sun.com/javase/6/docs/api/", "company", "oracle"))
			.
			add(createPactJsonObject("name", "Jane Doe", "url", "www.oracle.com", "company", "oracle")).
			add(createPactJsonObject("name", "Max Mustermann", "url", "www.oracle.com", "company", "oracle"));

		sopremoPlan.run();
	}
}
