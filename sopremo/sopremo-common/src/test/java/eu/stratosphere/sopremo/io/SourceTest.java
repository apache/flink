package eu.stratosphere.sopremo.io;

import org.junit.Test;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.TextNode;

public class SourceTest extends SopremoTest<Source> {
	@Override
	@Test
	public void shouldComplyEqualsContract() {
		super.shouldComplyEqualsContract(new Source(new ConstantExpression(0)), new Source(new ConstantExpression(1)),
			new Source("2"), new Source("3"));
	}

	@Test
	public void shouldGenerateAdhocInput() {
		final SopremoTestPlan plan = new SopremoTestPlan(new Source(new ConstantExpression(42)));
		plan.getExpectedOutput(0).add(IntNode.valueOf(42));
		plan.run();
	}

	@Test
	public void shouldGenerateMultipleAdhocInput() {
		final SopremoTestPlan plan =
			new SopremoTestPlan(new Source(
				new ArrayCreation(new ConstantExpression(42), new ConstantExpression("test"))));
		plan.getExpectedOutput(0).
			add(IntNode.valueOf(42)).
			add(TextNode.valueOf("test"));
		plan.run();
	}
}
