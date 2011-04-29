package eu.stratosphere.simple.jaql;

import org.junit.Test;

import eu.stratosphere.sopremo.Comparison.BinaryOperator;
import eu.stratosphere.sopremo.Comparison;
import eu.stratosphere.sopremo.BooleanExpression;
import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.expressions.Transformation;
import eu.stratosphere.sopremo.expressions.ValueAssignment;
import eu.stratosphere.sopremo.operator.Join;
import eu.stratosphere.sopremo.operator.Source;

public class JoinTest extends ParserTestCase {
	private static String usersJaql() {
		return "users  =[  {name: \"Jon Doe\", password: \"asdf1234\", id: 1},  {name: \"Jane Doe\", password: \"qwertyui\", id: 2},  {name: \"Max Mustermann\", password: \"q1w2e3r4\", id: 3}];"
			+ "pages = [  {userid: 1, url:\"code.google.com/p/jaql/\"},  {userid: 2, url:\"www.cnn.com\"},  {userid: 1, url:\"java.sun.com/javase/6/docs/api/\"}];";
	}

	private static Source usersSource() {
		return new Source(createJsonArray(
			createObject("name", "Jon Doe", "password", "asdf1234", "id", 1L),
			createObject("name", "Jane Doe", "password", "qwertyui", "id", 2L),
			createObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3L)));
	}

	private static Source usersPages() {
		return new Source(createJsonArray(
			createObject("userid", 1L, "url", "code.google.com/p/jaql/"),
			createObject("userid", 2L, "url", "www.cnn.com"),
			createObject("userid", 1L, "url", "java.sun.com/javase/6/docs/api/")));
	}

	@Test
	public void shouldParseEquiJoin() {
		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.EQUAL,
			createPath("1", "userid")));

		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment("name", createPath("0", "name")));
		transformation.addMapping(new ValueAssignment(ValueAssignment.COPY_ALL_FIELDS, createPath("1")));

		assertParseResult(new Join(transformation, condition, usersSource(), usersPages()),
			usersJaql() + "join users, pages where users.id == pages.userid into {users.name, pages.*};");
	}

	@Test
	public void shouldParseEquiJoinWithIterationVariables() {
		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.EQUAL,
			createPath("1", "userid")));

		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment("name", createPath("0", "name")));
		transformation.addMapping(new ValueAssignment(ValueAssignment.COPY_ALL_FIELDS, createPath("1")));

		assertParseResult(new Join(transformation, condition, usersSource(), usersPages()),
			usersJaql() + "join u in users, p in pages where u.id == p.userid into {u.name, p.*};");
	}

	@Test
	public void shouldParseLeftOuterEquiJoinWithIterationVariables() {
		Condition condition = new Condition(new Comparison(createPath("0", "id"), BinaryOperator.EQUAL,
			createPath("1", "userid")));

		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment("name", createPath("0", "name")));
		transformation.addMapping(new ValueAssignment(ValueAssignment.COPY_ALL_FIELDS, createPath("1")));

		Source usersSource = usersSource();
		assertParseResult(new Join(transformation, condition, usersSource, usersPages()).withOuterJoin(usersSource),
			usersJaql() + "join preserve u in users, p in pages where u.id == p.userid into {u.name, p.*};");
	}
}
