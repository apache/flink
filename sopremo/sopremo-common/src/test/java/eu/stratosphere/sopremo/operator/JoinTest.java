package eu.stratosphere.sopremo.operator;

import org.junit.Test;

import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.sopremo.Comparison;
import eu.stratosphere.sopremo.Comparison.BinaryOperator;
import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.ElementExpression;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.SopremoTestPlan;
import eu.stratosphere.sopremo.expressions.Transformation;

public class JoinTest extends SopremoTest {

	@Test
	public void shouldPerformEquiJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1",
			"userid")));
		Join join = new Join(Transformation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputOperators(join);

		sopremoPlan.getInput(0).
			add(createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1L)).
			add(createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2L)).
			add(createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3L));
		sopremoPlan.getInput(1).
			add(createJsonObject("userid", 1L, "url", "code.google.com/p/jaql/")).
			add(createJsonObject("userid", 2L, "url", "www.cnn.com")).
			add(createJsonObject("userid", 1L, "url", "java.sun.com/javase/6/docs/api/"));
		sopremoPlan.getOutput(0).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1L, "userid", 1L, "url",
					"code.google.com/p/jaql/")).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1L, "userid", 1L, "url",
					"code.google.com/p/jaql/")).
			addExpected(
				createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2L, "userid", 2L, "url",
					"java.sun.com/javase/6/docs/api/"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformLeftOuterJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1",
			"userid")));
		Join join = new Join(Transformation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		// here we set outer join flag
		join.setOuterJoin(sopremoPlan.getInputOperator(0));
		sopremoPlan.getOutputOperator(0).setInputOperators(join);

		sopremoPlan.getInput(0).
			add(createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1L)).
			add(createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2L)).
			add(createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3L));
		sopremoPlan.getInput(1).
			add(createJsonObject("userid", 1L, "url", "code.google.com/p/jaql/")).
			add(createJsonObject("userid", 2L, "url", "www.cnn.com")).
			add(createJsonObject("userid", 1L, "url", "java.sun.com/javase/6/docs/api/"));
		sopremoPlan.getOutput(0).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1L, "userid", 1L, "url",
					"code.google.com/p/jaql/")).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1L, "userid", 1L, "url",
					"code.google.com/p/jaql/")).
			addExpected(
				createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2L, "userid", 2L, "url",
					"java.sun.com/javase/6/docs/api/")).
			addExpected(
				createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3L));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformRightOuterJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1",
			"userid")));
		Join join = new Join(Transformation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		// here we set outer join flag
		join.setOuterJoin(sopremoPlan.getInputOperator(1));
		sopremoPlan.getOutputOperator(0).setInputOperators(join);

		sopremoPlan.getInput(0).
			add(createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1L)).
			add(createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2L)).
			add(createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3L));
		sopremoPlan.getInput(1).
			add(createJsonObject("userid", 1L, "url", "code.google.com/p/jaql/")).
			add(createJsonObject("userid", 2L, "url", "www.cnn.com")).
			add(createJsonObject("userid", 4L, "url", "www.nbc.com")).
			add(createJsonObject("userid", 1L, "url", "java.sun.com/javase/6/docs/api/"));
		sopremoPlan.getOutput(0).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1L, "userid", 1L, "url",
					"code.google.com/p/jaql/")).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1L, "userid", 1L, "url",
					"code.google.com/p/jaql/")).
			addExpected(
				createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2L, "userid", 2L, "url",
					"java.sun.com/javase/6/docs/api/")).
			addExpected(
				createJsonObject("userid", 4L, "url", "www.nbc.com"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformFullOuterJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1",
			"userid")));
		Join join = new Join(Transformation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		// here we set outer join flag
		join.setOuterJoin(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputOperators(join);

		sopremoPlan.getInput(0).
			add(createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1L)).
			add(createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2L)).
			add(createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3L));
		sopremoPlan.getInput(1).
			add(createJsonObject("userid", 1L, "url", "code.google.com/p/jaql/")).
			add(createJsonObject("userid", 2L, "url", "www.cnn.com")).
			add(createJsonObject("userid", 4L, "url", "www.nbc.com")).
			add(createJsonObject("userid", 1L, "url", "java.sun.com/javase/6/docs/api/"));
		sopremoPlan.getOutput(0).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1L, "userid", 1L, "url",
					"code.google.com/p/jaql/")).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1L, "userid", 1L, "url",
					"code.google.com/p/jaql/")).
			addExpected(
				createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2L, "userid", 2L, "url",
					"java.sun.com/javase/6/docs/api/")).
			addExpected(
				createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3L)).
			addExpected(
				createJsonObject("userid", 4L, "url", "www.nbc.com"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformThetaJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.LESS, createPath("1",
			"userid")));
		Join join = new Join(Transformation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		// here we set outer join flag
		join.setOuterJoin(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputOperators(join);

		sopremoPlan.getInput(0).
			add(createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1L)).
			add(createJsonObject("name", "Jane Doe", "password", "qwertyui", "id", 2L)).
			add(createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3L));
		sopremoPlan.getInput(1).
			add(createJsonObject("userid", 1L, "url", "code.google.com/p/jaql/")).
			add(createJsonObject("userid", 2L, "url", "www.cnn.com")).
			add(createJsonObject("userid", 4L, "url", "www.nbc.com")).
			add(createJsonObject("userid", 1L, "url", "java.sun.com/javase/6/docs/api/"));
		sopremoPlan.getOutput(0).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1L, "userid", 2L, "url",
					"www.cnn.com")).
			addExpected(
				createJsonObject("name", "Jon Doe", "password", "asdf1234", "id", 1L, "userid", 4L, "url",
					"www.nbc.com")).
			addExpected(
				createJsonObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3L, "url", "www.nbc.com"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformAntiJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new ElementExpression(createPath("0", "DeptName"), createPath("1", "Name"),
			false));
		Join join = new Join(Transformation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		// here we set outer join flag
		join.setOuterJoin(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputOperators(join);

		sopremoPlan.getInput(0).
			add(createJsonObject("Name", "Harry", "EmpId", 3415L, "DeptName", "Finance")).
			add(createJsonObject("Name", "Sally", "EmpId", 2241L, "DeptName", "Sales")).
			add(createJsonObject("Name", "George", "EmpId", 3401L, "DeptName", "Finance")).
			add(createJsonObject("Name", "Harriet", "EmpId", 2202L, "DeptName", "Production"));
		sopremoPlan.getInput(1).
			add(createJsonObject("Name", "Sales", "Manager", "Harriet")).
			add(createJsonObject("Name", "Production", "Manager", "Charles"));
		sopremoPlan.getOutput(0).
			addExpected(createJsonObject("Name", "Harry", "EmpId", 3415L, "DeptName", "Finance")).
			addExpected(createJsonObject("Name", "George", "EmpId", 3401L, "DeptName", "Finance"));

		sopremoPlan.run();
	}

	@Test
	public void shouldPerformSemiJoin() {
		SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 1);

		Condition condition = new Condition(new ElementExpression(createPath("0", "DeptName"), createPath("1", "Name"),
			true));
		Join join = new Join(Transformation.CONCATENATION, condition, sopremoPlan.getInputOperators(0, 2));
		// here we set outer join flag
		join.setOuterJoin(sopremoPlan.getInputOperators(0, 2));
		sopremoPlan.getOutputOperator(0).setInputOperators(join);

		sopremoPlan.getInput(0).
			add(createJsonObject("Name", "Harry", "EmpId", 3415L, "DeptName", "Finance")).
			add(createJsonObject("Name", "Sally", "EmpId", 2241L, "DeptName", "Sales")).
			add(createJsonObject("Name", "George", "EmpId", 3401L, "DeptName", "Finance")).
			add(createJsonObject("Name", "Harriet", "EmpId", 2202L, "DeptName", "Production"));
		sopremoPlan.getInput(1).
			add(createJsonObject("Name", "Sales", "Manager", "Harriet")).
			add(createJsonObject("Name", "Production", "Manager", "Charles"));
		sopremoPlan.getOutput(0).
			addExpected(createJsonObject("Name", "Sally", "EmpId", 2241L, "DeptName", "Sales")).
			addExpected(createJsonObject("Name", "Harriet", "EmpId", 2202L, "DeptName", "Production"));

		sopremoPlan.run();
	}
}
