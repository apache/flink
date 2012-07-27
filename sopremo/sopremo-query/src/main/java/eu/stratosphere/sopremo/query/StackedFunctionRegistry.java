package eu.stratosphere.sopremo.query;

import java.lang.reflect.Method;

import eu.stratosphere.sopremo.function.Callable;
import eu.stratosphere.sopremo.packages.DefaultFunctionRegistry;
import eu.stratosphere.sopremo.packages.IFunctionRegistry;

public class StackedFunctionRegistry extends StackedRegistry<Callable<?, ?>, IFunctionRegistry> implements
		IFunctionRegistry {

	public StackedFunctionRegistry() {
		super(new DefaultFunctionRegistry());
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 7521011157999074896L;

	@Override
	public void put(Method method) {
		this.getTopRegistry().put(method);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.packages.IFunctionRegistry#put(java.lang.String, java.lang.Class, java.lang.String)
	 */
	@Override
	public void put(String registeredName, Class<?> clazz, String staticMethodName) {
		this.getTopRegistry().put(registeredName, clazz, staticMethodName);
	}

	@Override
	public void put(Class<?> javaFunctions) {
		this.getTopRegistry().put(javaFunctions);
	}
}