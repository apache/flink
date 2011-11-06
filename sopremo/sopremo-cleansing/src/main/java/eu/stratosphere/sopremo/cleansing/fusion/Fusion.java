package eu.stratosphere.sopremo.cleansing.fusion;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMaps;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Property;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.cleansing.scrubbing.CleansingRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.DefaultRuleFactory;
import eu.stratosphere.sopremo.cleansing.scrubbing.ExpressionRewriter;
import eu.stratosphere.sopremo.cleansing.scrubbing.RewriteContext;
import eu.stratosphere.sopremo.cleansing.scrubbing.RuleFactory;
import eu.stratosphere.sopremo.cleansing.scrubbing.RuleManager;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.JsonStreamExpression;
import eu.stratosphere.sopremo.expressions.MethodCall;
import eu.stratosphere.sopremo.expressions.MethodPointerExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.function.SimpleMacro;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * Input elements are either
 * <ul>
 * <li>Array of records resulting from record linkage without transitive closure. [r1, r2, r3] with r<sub>i</sub>
 * <li>Array of record clusters resulting from record linkage with transitive closure
 * </ul>
 * 
 * @author Arvid Heise
 */
@Name(verb = "fuse")
public class Fusion extends CompositeOperator<Fusion> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8429199636646276642L;

	private static final DefaultRuleFactory FusionRuleFactory = new DefaultRuleFactory();
	
	{
		FusionRuleFactory.addRewriteRule(new ExpressionRewriter.ExpressionType(MethodPointerExpression.class),
			new SimpleMacro<MethodPointerExpression>() {
				private static final long serialVersionUID = -8260133422163585840L;

				/*
				 * (non-Javadoc)
				 * @see eu.stratosphere.sopremo.function.SimpleMacro#call(eu.stratosphere.sopremo.expressions.
				 * EvaluationExpression, eu.stratosphere.sopremo.EvaluationContext)
				 */
				@Override
				public EvaluationExpression call(MethodPointerExpression inputExpr, EvaluationContext context) {
					return new MethodCall(inputExpr.getFunctionName(), ((RewriteContext) context).getRewritePath());
				}
			});
		FusionRuleFactory.addRewriteRule(new ExpressionRewriter.ExpressionType(JsonStreamExpression.class),
			new SimpleMacro<JsonStreamExpression>() {
				private static final long serialVersionUID = 111389216483477521L;

				/*
				 * (non-Javadoc)
				 * @see eu.stratosphere.sopremo.function.SimpleMacro#call(eu.stratosphere.sopremo.expressions.
				 * EvaluationExpression, eu.stratosphere.sopremo.EvaluationContext)
				 */
				@Override
				public EvaluationExpression call(JsonStreamExpression inputExpr, EvaluationContext context) {
					if (inputExpr.getStream() == context.getCurrentOperator())
						return ((RewriteContext) context).getRewritePath();
					return EvaluationExpression.VALUE;
				}
			});
		FusionRuleFactory.addRewriteRule(new ExpressionRewriter.ExpressionType(CleansingRule.class),
			new SimpleMacro<CleansingRule<?>>() {
				private static final long serialVersionUID = 111389216483477521L;

				/*
				 * (non-Javadoc)
				 * @see eu.stratosphere.sopremo.function.SimpleMacro#call(eu.stratosphere.sopremo.expressions.
				 * EvaluationExpression, eu.stratosphere.sopremo.EvaluationContext)
				 */
				@Override
				public EvaluationExpression call(CleansingRule<?> rule, EvaluationContext context) {
					PathExpression path = ((RewriteContext) context).getRewritePath();
					path.add(rule);
					return  path;
				}
			});
	}

	private final List<Object2DoubleMap<PathExpression>> weights = new ArrayList<Object2DoubleMap<PathExpression>>();

	private RuleManager fusionRules = new RuleManager(), updateRules = new RuleManager();
	
	private boolean multipleRecordsPerSource = false;

	private FusionRule defaultValueRule = MergeRule.INSTANCE;

	public FusionRule getDefaultValueRule() {
		return this.defaultValueRule;
	}


	private Object2DoubleMap<PathExpression> getWeightMap(final int inputIndex) {
		Object2DoubleMap<PathExpression> weightMap = this.weights.get(inputIndex);
		if (weightMap == null) {
			while (this.weights.size() <= inputIndex)
				this.weights.add(null);
			this.weights.set(inputIndex, weightMap = new Object2DoubleOpenHashMap<PathExpression>());
			weightMap.defaultReturnValue(1);
		}
		return weightMap;
	}

	public double getWeights(final int inputIndex, final String... path) {
		return this.getWeightMap(inputIndex).getDouble(Arrays.asList(path));
	}

	public boolean isMultipleRecordsPerSource() {
		return this.multipleRecordsPerSource;
	}

	public void addFusionRule(EvaluationExpression rule, List<EvaluationExpression> target) {
		fusionRules.addRule(rule, target);
	}

	public void addFusionRule(EvaluationExpression rule, EvaluationExpression... target) {
		fusionRules.addRule(rule, target);
	}

	public void removeFusionRule(EvaluationExpression rule, List<EvaluationExpression> target) {
		fusionRules.removeRule(rule, target);
	}

	public void removeFusionRule(EvaluationExpression rule, EvaluationExpression... target) {
		fusionRules.removeRule(rule, target);
	}

	public void setDefaultValueRule(final FusionRule defaultValueRule) {
		if (defaultValueRule == null)
			throw new NullPointerException("defaultValueRule must not be null");

		this.defaultValueRule = defaultValueRule;
	}

	public void setMultipleRecordsPerSource(final boolean multipleRecordsPerSource) {
		this.multipleRecordsPerSource = multipleRecordsPerSource;
	}

	public void setWeight(final double weight, final int inputIndex, final PathExpression path) {
		this.getWeightMap(inputIndex).put(path, weight);
	}

	@Property
	@Name(preposition = "into")
	public void setFusionExpression(ObjectCreation ruleExpression) {
		fusionRules.parse(ruleExpression, this, FusionRuleFactory);
		System.out.println(ruleExpression);
//		this.rules.parse(ruleExpression, );
		// extractRules(ruleExpression, EvaluationExpression.VALUE);
	}

	public ObjectCreation getFusionExpression() {
		return (ObjectCreation) fusionRules.getLastParsedExpression();
	}

	@Property
	@Name(preposition = "with weights")
	public void setWeightExpression(ObjectCreation ruleExpression) {
		System.out.println(ruleExpression);
//		this.rules.clear();
		// extractRules(ruleExpression, EvaluationExpression.VALUE);
	}

	public ObjectCreation getWeightExpression() {
		return new ObjectCreation();
	}

	@Property
	@Name(verb = "update")
	public void setUpdateExpression(ObjectCreation ruleExpression) {
		System.out.println(ruleExpression);
//		this.rules.clear();
		// extractRules(ruleExpression, EvaluationExpression.VALUE);
	}

	@Property
	@Name(verb = "update")
	public ObjectCreation getUpdateExpression() {
		return new ObjectCreation();
	}
	
	@Override
	public SopremoModule asElementaryOperators() {
		return SopremoModule.valueOf("fusion", new ValueFusion().withFusionRules(fusionRules));
	}
	
	public static class ValueFusion extends ElementaryOperator<ValueFusion> {
		private RuleManager fusionRules = new RuleManager();
		
public void setFusionRules(RuleManager fusionRules) {
	if (fusionRules == null)
		throw new NullPointerException("fusionRules must not be null");

	this.fusionRules = fusionRules;
}

public RuleManager getFusionRules() {
	return fusionRules;
}

public ValueFusion withFusionRules(RuleManager fusionRules) {
	setFusionRules(fusionRules);
	return this;
}

	public static class Implementation extends
			SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {
		private RuleManager fusionRules = new RuleManager();

		private List<Object2DoubleMap<PathExpression>> weights;

		private FusionContext context;

		private boolean multipleRecordsPerSource;

		private FusionRule defaultValueRule;

		private transient List<JsonNode> contextNodes = new ArrayList<JsonNode>();

		@Override
		public void configure(final Configuration parameters) {
			super.configure(parameters);

			this.context = new FusionContext(this.getContext());
			for (int index = 0; index < this.weights.size(); index++)
				if (this.weights.get(index) == null || this.weights.get(index).isEmpty()) {
					final Object2DoubleMap<PathExpression> quickMap = Object2DoubleMaps.singleton(null, null);
					quickMap.defaultReturnValue(1);
					this.weights.set(index, quickMap);
				}
		}

		private JsonNode findFirstNode(final JsonNode[] values) {
			JsonNode firstNonNull = NullNode.getInstance();
			for (final JsonNode value : values)
				if (value != NullNode.getInstance()) {
					firstNonNull = value;
					break;
				}
			return firstNonNull;
		}

		private JsonNode fuse(final JsonNode[] values, final double[] weights, final PathExpression currentPath) {
			UnresolvedNodes unresolved = new UnresolvedNodes(values);
			for(EvaluationExpression fusionRule : this.fusionRules.get(currentPath)) {
				this.context.setWeights(weights);
				JsonNode resolvedNode = fusionRule.evaluate(unresolved, this.context);
				if(resolvedNode instanceof UnresolvedNodes)
					unresolved = (UnresolvedNodes) resolvedNode;
				else return resolvedNode;
			}

			final JsonNode firstNonNull = this.findFirstNode(values);
			if (firstNonNull == NullNode.getInstance())
				return firstNonNull;

			if (firstNonNull.isObject())
				return this.fuseObjects(values, weights, currentPath, ((ObjectNode) firstNonNull).getFieldNames());

			if (firstNonNull.isArray())
				return this.fuseArrays(values);

			return this.defaultValueRule.fuse(values, weights, this.context);
		}

		private JsonNode fuseArrays(final JsonNode[] values) {
			final ArrayNode fusedArray = new ArrayNode();
			for (final JsonNode array : values)
				for (final JsonNode element : (ArrayNode) array)
					fusedArray.add(element);
			return fusedArray;
		}

		private JsonNode fuseObjects(final JsonNode[] values, final double[] weights, final PathExpression currentPath,
				final Iterator<String> fieldNames) {
			final JsonNode[] children = new JsonNode[values.length];
			final double[] childWeights = new double[weights.length];

			final ObjectNode fusedObject = new ObjectNode();
			while (fieldNames.hasNext()) {
				final String fieldName = fieldNames.next();

				for (int index = 0; index < values.length; index++) {
					children[index] = values[index] == NullNode.getInstance() ? values[index]
						: ((ObjectNode) values[index])
							.get(fieldName);
					childWeights[index] = weights[index] * this.getWeight(index, currentPath);
				}

				currentPath.add(new ObjectAccess(fieldName));
				fusedObject.put(fieldName, this.fuse(children, childWeights, currentPath));
				currentPath.removeLast();
			}

			return fusedObject;
		}

		private Double getWeight(final int index, final PathExpression path) {
			return this.weights.get(this.context.getSourceIndexes()[index]).get(path);
		}

		@Override
		protected void map(final JsonNode key, final JsonNode values, final JsonCollector out) {
			try {
				this.contextNodes.clear();
				if (this.multipleRecordsPerSource) {
					final Iterator<JsonNode> iterator = ((ArrayNode) values).iterator();
					final IntList sourceIndexes = new IntArrayList();
					for (int sourceIndex = 0; iterator.hasNext(); sourceIndex++)
						for (final JsonNode value : (ArrayNode) iterator.next()) {
							this.contextNodes.add(value);
							sourceIndexes.add(sourceIndex);
						}

					this.context.setSourceIndexes(sourceIndexes.toIntArray());
				} else {
					for (final JsonNode value : (ArrayNode) values)
						this.contextNodes.add(value);

					final int[] sourceIndexes = new int[this.contextNodes.size()];
					for (int index = 0; index < sourceIndexes.length; index++)
						sourceIndexes[index] = index;

					this.context.setSourceIndexes(sourceIndexes);
				}

				this.context.setContextNodes(this.contextNodes.toArray(new JsonNode[this.contextNodes.size()]));
				final double[] initialWeights = new double[this.contextNodes.size()];
				for (int index = 0; index < initialWeights.length; index++)
					initialWeights[index] = this.getWeight(index, new PathExpression());
				out.collect(key, this.fuse(this.context.getContextNodes(), initialWeights, new PathExpression()));
			} catch (final UnresolvableEvaluationException e) {
				// do not emit invalid record
			}
		}
	}
	}

}
