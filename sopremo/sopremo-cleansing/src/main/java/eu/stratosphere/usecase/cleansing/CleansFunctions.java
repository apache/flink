package eu.stratosphere.usecase.cleansing;

import java.util.concurrent.atomic.AtomicLong;

import uk.ac.shef.wit.simmetrics.similaritymetrics.InterfaceStringMetric;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaccardSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler;
import eu.stratosphere.sopremo.BuiltinProvider;
import eu.stratosphere.sopremo.ConstantRegistryCallback;
import eu.stratosphere.sopremo.DefaultFunctions;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.FunctionRegistryCallback;
import eu.stratosphere.sopremo.cleansing.scrubbing.NonNullRule;
import eu.stratosphere.sopremo.cleansing.similarity.SimmetricFunction;
import eu.stratosphere.sopremo.expressions.UnevaluableExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.function.MacroBase;
import eu.stratosphere.sopremo.function.MethodRegistry;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.TextNode;

public class CleansFunctions implements BuiltinProvider, ConstantRegistryCallback, FunctionRegistryCallback {
	private static AtomicLong Id = new AtomicLong();

	@Override
	public void registerConstants(EvaluationContext context) {
		context.getBindings().set("required", new NonNullRule());
		// bindings.set("required", new NonNullRule());
	}

	@Override
	public void registerFunctions(MethodRegistry registry) {
		registry.register(new SimmetricMacro("jaccard", new JaccardSimilarity()));
		registry.register(new SimmetricMacro("jaroWinkler", new JaroWinkler()));
		registry.register(new VoteMacro());
	}

	public static JsonNode generateId(TextNode prefix) {
		return TextNode.valueOf(prefix.getTextValue() + Id.incrementAndGet());
	}

	public static JsonNode removeVowels(TextNode node) {
		return DefaultFunctions.replace(node, TextNode.valueOf("(?i)[aeiou]"), TextNode.valueOf(""));
	}

	public static JsonNode longest(ArrayNode values) {
		JsonNode longest = values.get(0);
		int longestLength = ((TextNode) longest).getJavaValue().length();

		ArrayNode result = new ArrayNode();
		for (int index = 1; index < values.size(); index++) {
			JsonNode node = values.get(1);
			int length = ((TextNode) node).getJavaValue().length();

			if (longestLength > length) {
				result.clear();
				longestLength = length;
			}
			if (longestLength == length)
				result.add(node);
		}
		return result;
	}
	
	public static JsonNode first(ArrayNode values) {
		return values.subList(0, 1);
	}

	/**
	 * @author Arvid Heise
	 */
	private static class SimmetricMacro extends MacroBase {
		/**
		 * 
		 */
		private static final long serialVersionUID = -2086644180411129824L;

		private InterfaceStringMetric metric;

		/**
		 * Initializes SimmetricMacro.
		 * 
		 * @param name
		 * @param metric
		 */
		public SimmetricMacro(String name, InterfaceStringMetric metric) {
			super(name);
			this.metric = metric;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object,
		 * eu.stratosphere.sopremo.EvaluationContext)
		 */
		@Override
		public EvaluationExpression call(EvaluationExpression[] params, EvaluationContext context) {
			if (params.length > 1)
				return new SimmetricFunction(this.metric, params[0], params[1]);
			return new SimmetricFunction(this.metric, params[0], params[0]);
		}
	}

	private static class VoteMacro extends MacroBase {
		/**
		 * 
		 */
		private static final long serialVersionUID = -2673091978964835311L;

		/**
		 * Initializes CleansFunctions.VoteMacro.
		 */
		public VoteMacro() {
			super("vote");
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object,
		 * eu.stratosphere.sopremo.EvaluationContext)
		 */
		@Override
		public EvaluationExpression call(EvaluationExpression[] params, EvaluationContext context) {
			return new UnevaluableExpression("not implemented");
		}

	}
}
