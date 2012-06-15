package eu.stratosphere.sopremo;

import org.junit.Test;

import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IntNode;

public class SourceTest extends SopremoTest<Source> {
	@Override
	@Test
	public void shouldComplyEqualsContract() {
		super.shouldComplyEqualsContract(new Source(new ConstantExpression(0)), new Source(new ConstantExpression(1)),
			new Source("2"), new Source("3"));
	}

	@Test
	public void shouldGenerateAdhocInput() {
		SopremoTestPlan plan = new SopremoTestPlan(new Source(new ConstantExpression(42)));
		plan.getExpectedOutput(0).add(IntNode.valueOf(42));
		plan.run();
	}
}
