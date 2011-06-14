package eu.stratosphere.simple.jaql;

import org.junit.Test;

import eu.stratosphere.sopremo.base.Join;
import eu.stratosphere.sopremo.base.Source;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConditionalExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;

public class JoinTest extends ParserTestCase {
	private static String usersJaql() {
		return "users  =[  {name: \"Jon Doe\", password: \"asdf1234\", id: 1},  {name: \"Jane Doe\", password: \"qwertyui\", id: 2},  {name: \"Max Mustermann\", password: \"q1w2e3r4\", id: 3}];"
			+ "pages = [  {userid: 1, url:\"code.google.com/p/jaql/\"},  {userid: 2, url:\"www.cnn.com\"},  {userid: 1, url:\"java.sun.com/javase/6/docs/api/\"}];";
	}

	private static Source usersSource() {
		return new Source(createJsonArray(createObject("name", "Jon Doe", "password", "asdf1234", "id", 1L),
			createObject("name", "Jane Doe", "password", "qwertyui", "id", 2L),
			createObject("name", "Max Mustermann", "password", "q1w2e3r4", "id", 3L)));
	}

	private static Source usersPages() {
		return new Source(createJsonArray(createObject("userid", 1L, "url", "code.google.com/p/jaql/"),
			createObject("userid", 2L, "url", "www.cnn.com"),
			createObject("userid", 1L, "url", "java.sun.com/javase/6/docs/api/")));
	}

	@Test
	public void shouldParseEquiJoin() {
		ConditionalExpression condition = new ConditionalExpression(new ComparativeExpression(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1",
			"userid")));

		ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("name", createPath("0", "name"));
		transformation.addMapping(new ObjectCreation.CopyFields(createPath("1")));

		assertParseResult(new Join(transformation, condition, usersSource(), usersPages()), usersJaql()
			+ "join users, pages where users.id == pages.userid into {users.name, pages.*};");
	}

	@Test
	public void shouldParseEquiJoinWithIterationVariables() {
		ConditionalExpression condition = new ConditionalExpression(new ComparativeExpression(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1",
			"userid")));

		ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("name", createPath("0", "name"));
		transformation.addMapping(new ObjectCreation.CopyFields(createPath("1")));

		assertParseResult(new Join(transformation, condition, usersSource(), usersPages()), usersJaql()
			+ "join u in users, p in pages where u.id == p.userid into {u.name, p.*};");
	}

	@Test
	public void shouldParseLeftOuterEquiJoinWithIterationVariables() {
		ConditionalExpression condition = new ConditionalExpression(new ComparativeExpression(createPath("0", "id"), BinaryOperator.EQUAL, createPath("1",
			"userid")));

		ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("name", createPath("0", "name"));
		transformation.addMapping(new ObjectCreation.CopyFields(createPath("1")));

		Source usersSource = usersSource();
		assertParseResult(new Join(transformation, condition, usersSource, usersPages()).withOuterJoin(usersSource),
			usersJaql() + "join preserve u in users, p in pages where u.id == p.userid into {u.name, p.*};");
	}
}
