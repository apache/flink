package eu.stratosphere.usecase.cleansing;

import uk.ac.shef.wit.simmetrics.similaritymetrics.JaccardSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler;
import eu.stratosphere.sopremo.BuiltinProvider;
import eu.stratosphere.sopremo.ConstantRegistryCallback;
import eu.stratosphere.sopremo.DefaultFunctions;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.FunctionRegistryCallback;
import eu.stratosphere.sopremo.cleansing.scrubbing.NonNullRule;
import eu.stratosphere.sopremo.cleansing.similarity.SimmetricFunction;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.function.FunctionRegistry;
import eu.stratosphere.sopremo.function.SopremoFunction;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.TextNode;

public class CleansFunctions implements BuiltinProvider, ConstantRegistryCallback, FunctionRegistryCallback {
	public void vote(JsonNode... possibleValues) {

	}

	@Override
	public void registerFunctions(FunctionRegistry registry) {
		registry.register(new SopremoFunction("jaccard", new SimmetricFunction(new JaccardSimilarity(), EvaluationExpression.VALUE, EvaluationExpression.VALUE)));
		registry.register(new SopremoFunction("jaroWinkler", new SimmetricFunction(new JaroWinkler(), EvaluationExpression.VALUE, EvaluationExpression.VALUE)));
	}
	
	public static JsonNode removeVowels(TextNode node) {
		return DefaultFunctions.replace(node, TextNode.valueOf("(?i)[aeiou]"), TextNode.valueOf(""));
	}

	@Override
	public void registerConstants(EvaluationContext context) {
		context.setBinding("required", new NonNullRule());
		// bindings.set("required", new NonNullRule());
	}
}
