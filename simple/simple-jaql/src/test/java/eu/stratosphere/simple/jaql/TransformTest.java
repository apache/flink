package eu.stratosphere.simple.jaql;

import org.junit.Test;

import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Source;
import eu.stratosphere.sopremo.expressions.Arithmetic;
import eu.stratosphere.sopremo.expressions.Arithmetic.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ObjectCreation;

public class TransformTest extends ParserTestCase {
	private static String recordsJaql() {
		return "recs = [  {a: 1, b: 4},  {a: 2, b: 5},  {a: -1, b: 4}];";
	}

	private static Source recordsSource() {
		return new Source(createJsonArray(createObject("a", 1L, "b", 4L), createObject("a", 2L, "b", 5L),
			createObject("a", -1L, "b", 4L)));
	}

	@Test
	public void shouldParseEmptyTransform() {
		assertParseResult(new Projection(new ObjectCreation(), new Source(createJsonArray(1L, 2L, 3L))),
			"[1, 2, 3] -> transform {}");
	}

	@Test
	public void shouldParseSimpleTransform() {
		Source source = new Source(createJsonArray(createObject("a", 1L, "b", 2L), createObject("a", 3L, "b", 4L)));
		ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("c", new Arithmetic(createPath("$", "a"),
			ArithmeticOperator.MULTIPLY, createPath("$", "b")));
		assertParseResult(new Projection(transformation, source),
			"[{a: 1, b: 2}, {a: 3, b: 4}] -> transform { c: $.a * $.b }");
	}

	@Test
	public void shouldParseNestedTransform() {
		Source source = new Source(createJsonArray(createObject("a", 1L, "b", 2L), createObject("a", 3L, "b", 4L)));
		ObjectCreation transformation = new ObjectCreation();
		ObjectCreation nestedObjectCreation = new ObjectCreation();
		transformation.addMapping("c", nestedObjectCreation);
		nestedObjectCreation.addMapping("a", createPath("$", "a"));
		nestedObjectCreation.addMapping("d", createPath("$", "b"));
		assertParseResult(new Projection(transformation, source),
			"[{a: 1, b: 2}, {a: 3, b: 4}] -> transform { c: { $.a, d: $.b} }");
	}

	@Test
	public void shouldParseExampleTransform() {
		ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("sum", new Arithmetic(createPath("$", "a"),
			ArithmeticOperator.PLUS, createPath("$", "b")));
		assertParseResult(new Projection(transformation, recordsSource()), recordsJaql()
			+ "recs -> transform {sum: $.a + $.b}");
	}

	@Test
	public void shouldParseExampleTransformWithIterationVariable() {
		ObjectCreation transformation = new ObjectCreation();
		transformation.addMapping("sum", new Arithmetic(createPath("$", "a"),
			ArithmeticOperator.PLUS, createPath("$", "b")));
		assertParseResult(new Projection(transformation, recordsSource()), recordsJaql()
			+ "recs -> transform each r {sum: r.a + r.b}");
	}
}
