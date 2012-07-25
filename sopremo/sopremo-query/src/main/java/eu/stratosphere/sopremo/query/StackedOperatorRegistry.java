package eu.stratosphere.sopremo.query;

import eu.stratosphere.sopremo.operator.Operator;

final class StackedOperatorRegistry extends StackedRegistry<OperatorInfo<?>, IOperatorRegistry> implements
		IOperatorRegistry {

	public StackedOperatorRegistry() {
		super(new DefaultOperatorRegistry());
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -5952860268066011445L;

	@Override
	public void put(Class<? extends Operator<?>> clazz) {
		this.getTopRegistry().put(clazz);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.query.IOperatorRegistry#getName(java.lang.Class)
	 */
	@Override
	public String getName(Class<? extends Operator<?>> operatorClass) {
		return this.getTopRegistry().getName(operatorClass);
	}
}