package eu.stratosphere.sopremo.query;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.packages.DefaultConstantRegistry;
import eu.stratosphere.sopremo.packages.IConstantRegistry;

final class StackedConstantRegistry extends StackedRegistry<EvaluationExpression, IConstantRegistry> implements IConstantRegistry {
	public StackedConstantRegistry() {
		super(new DefaultConstantRegistry());
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 7496813129214724902L;
}