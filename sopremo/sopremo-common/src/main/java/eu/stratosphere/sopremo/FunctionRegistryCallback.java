package eu.stratosphere.sopremo;

import eu.stratosphere.sopremo.function.FunctionRegistry;

public interface FunctionRegistryCallback {
	void registerFunctions(FunctionRegistry registry);
}
