package eu.stratosphere.sopremo.query;

import java.lang.reflect.Method;

import eu.stratosphere.sopremo.function.SopremoMethod;
import eu.stratosphere.sopremo.packages.DefaultMethodRegistry;
import eu.stratosphere.sopremo.packages.IMethodRegistry;

final class StackedMethodRegistry extends StackedRegistry<SopremoMethod, IMethodRegistry> implements IMethodRegistry {

	public StackedMethodRegistry() {
		super(new DefaultMethodRegistry());
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 7521011157999074896L;
	
	@Override
	public void put(Method method) {
		getTopRegistry().put(method);
	}
	
	@Override
	public void put(Class<?> javaFunctions) {
		getTopRegistry().put(javaFunctions);
	}
}