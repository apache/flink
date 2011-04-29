package eu.stratosphere.simple.jaql;

import org.junit.Test;

import eu.stratosphere.sopremo.expressions.Arithmetic;
import eu.stratosphere.sopremo.expressions.Arithmetic.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.Transformation;
import eu.stratosphere.sopremo.expressions.ValueAssignment;
import eu.stratosphere.sopremo.operator.Projection;
import eu.stratosphere.sopremo.operator.Source;

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
		assertParseResult(new Projection(new Transformation(), new Source(createJsonArray(1L, 2L, 3L))),
			"[1, 2, 3] -> transform {}");
	}

	@Test
	public void shouldParseSimpleTransform() {
		Source source = new Source(createJsonArray(createObject("a", 1L, "b", 2L), createObject("a", 3L, "b", 4L)));
		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment("c", new Arithmetic(createPath("$", "a"),
			ArithmeticOperator.MULTIPLY, createPath("$", "b"))));
		assertParseResult(new Projection(transformation, source),
			"[{a: 1, b: 2}, {a: 3, b: 4}] -> transform { c: $.a * $.b }");
	}

	@Test
	public void shouldParseNestedTransform() {
		Source source = new Source(createJsonArray(createObject("a", 1L, "b", 2L), createObject("a", 3L, "b", 4L)));
		Transformation transformation = new Transformation();
		Transformation nestedTransformation = new Transformation("c");
		transformation.addMapping(nestedTransformation);
		nestedTransformation.addMapping(new ValueAssignment("a", createPath("$", "a")));
		nestedTransformation.addMapping(new ValueAssignment("d", createPath("$", "b")));
		assertParseResult(new Projection(transformation, source),
			"[{a: 1, b: 2}, {a: 3, b: 4}] -> transform { c: { $.a, d: $.b} }");
	}

	@Test
	public void shouldParseExampleTransform() {
		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment("sum", new Arithmetic(createPath("$", "a"),
			ArithmeticOperator.PLUS, createPath("$", "b"))));
		assertParseResult(new Projection(transformation, recordsSource()), recordsJaql()
			+ "recs -> transform {sum: $.a + $.b}");
	}

	@Test
	public void shouldParseExampleTransformWithIterationVariable() {
		Transformation transformation = new Transformation();
		transformation.addMapping(new ValueAssignment("sum", new Arithmetic(createPath("$", "a"),
			ArithmeticOperator.PLUS, createPath("$", "b"))));
		assertParseResult(new Projection(transformation, recordsSource()), recordsJaql()
			+ "recs -> transform each r {sum: r.a + r.b}");
	}
}
