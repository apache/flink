package eu.stratosphere.sopremo;

import eu.stratosphere.sopremo.function.MethodRegistry;

public interface FunctionRegistryCallback {
	void registerFunctions(MethodRegistry registry);
}
