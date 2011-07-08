package eu.stratosphere.sopremo;

import eu.stratosphere.sopremo.expressions.ConstantExpression;

public class SourceTest extends SopremoTest<Source> {
	@Override
	public void shouldComplyEqualsContract() {
		super.shouldComplyEqualsContract(new Source(new ConstantExpression(0)), new Source(new ConstantExpression(1)),
			new Source(PersistenceType.HDFS, "2"), new Source(PersistenceType.HDFS, "3"));
	}
}
