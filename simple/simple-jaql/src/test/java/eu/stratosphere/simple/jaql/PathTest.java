package eu.stratosphere.simple.jaql;

import org.junit.Test;

import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.FieldAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.expressions.EvaluableExpression.*;

public class PathTest extends ParserTestCase {
	@Test
	public void shouldParseSimpleSpreadOperator() {
		assertParseResult(new PathExpression(new ObjectCreation(), new ArrayAccess()), "$in = {}; $in[*]");
	}

	@Test
	public void shouldParseSpreadOperatorWithFieldAccess() {
		assertParseResult(new PathExpression(new ObjectCreation(), new ArrayAccess(), new FieldAccess("field")),
			"$in = {}; $in[*].field");
	}

	@Test
	public void shouldParseDoubleSpreadOperatorWithFieldAccess() {
		assertParseResult(new PathExpression(new ObjectCreation(), new ArrayAccess(), new FieldAccess("field"),
			new FieldAccess("field2"), new ArrayAccess(), new FieldAccess("field3")),
			"$in = {}; $in[*].field.field2[*].field3");
	}
}
