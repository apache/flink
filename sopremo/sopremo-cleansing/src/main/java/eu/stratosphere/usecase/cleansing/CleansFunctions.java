package eu.stratosphere.usecase.cleansing;

import eu.stratosphere.sopremo.Bindings;
import eu.stratosphere.sopremo.BuiltinProvider;
import eu.stratosphere.sopremo.ConstantRegistryCallback;
import eu.stratosphere.sopremo.DefaultFunctions;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.scrubbing.NonNullRule;
import eu.stratosphere.sopremo.function.FunctionRegistry;
import eu.stratosphere.sopremo.type.JsonNode;

public class CleansFunctions implements BuiltinProvider, ConstantRegistryCallback {
	public void vote(JsonNode... possibleValues) {

	}


	@Override
	public void registerConstants(EvaluationContext context) {
		context.setBinding("required", new NonNullRule());
		// bindings.set("required", new NonNullRule());
	}
}
