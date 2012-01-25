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
import java.util.Map.Entry;

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
import eu.stratosphere.sopremo.cleansing.scrubbing.RuleManager;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.JsonStreamExpression;
import eu.stratosphere.sopremo.expressions.MethodCall;
import eu.stratosphere.sopremo.expressions.MethodPointerExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.expressions.SingletonExpression;
import eu.stratosphere.sopremo.function.SimpleMacro;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.NumericNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;
import eu.stratosphere.util.CollectionUtil;

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

	public static final EvaluationExpression CONTEXT_NODES = new SingletonExpression("<context>") {
		/**
		 * 
		 */
		private static final long serialVersionUID = -3340948936846733311L;

		@Override
		public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
			return ((FusionContext) context).getContextNodes();
		}

		@Override
		protected Object readResolve() {
			return CONTEXT_NODES;
		}
	};

	private static final DefaultRuleFactory FusionRuleFactory = new DefaultRuleFactory(),
			UpdateRuleFactory = new DefaultRuleFactory(),
			WeightRuleFactory = new DefaultRuleFactory();

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
					return new MethodCall(inputExpr.getFunctionName(), EvaluationExpression.VALUE);
				}
			});
		FusionRuleFactory.addRewriteRule(new ExpressionRewriter.ExpressionType(InputSelection.class),
			new SimpleMacro<InputSelection>() {
				private static final long serialVersionUID = 111389216483477521L;

				/*
				 * (non-Javadoc)
				 * @see eu.stratosphere.sopremo.function.SimpleMacro#call(eu.stratosphere.sopremo.expressions.
				 * EvaluationExpression, eu.stratosphere.sopremo.EvaluationContext)
				 */
				@Override
				public EvaluationExpression call(InputSelection inputExpr, EvaluationContext context) {
					if (inputExpr.hasTag(JsonStreamExpression.THIS_CONTEXT))
						return EvaluationExpression.VALUE;
					return CONTEXT_NODES;
				}
			});
		WeightRuleFactory.addRewriteRule(new ExpressionRewriter.ExpressionType(ArithmeticExpression.class),
			new SimpleMacro<ArithmeticExpression>() {
				private static final long serialVersionUID = 111389216483477521L;

				/*
				 * (non-Javadoc)
				 * @see eu.stratosphere.sopremo.function.SimpleMacro#call(eu.stratosphere.sopremo.expressions.
				 * EvaluationExpression, eu.stratosphere.sopremo.EvaluationContext)
				 */
				@Override
				public EvaluationExpression call(ArithmeticExpression arithExpr, EvaluationContext context) {
					if(arithExpr.getOperator() != ArithmeticOperator.MULTIPLICATION)
						throw new IllegalArgumentException("unsupported arithmetic expression");
					
					((RewriteContext)context).parse(arithExpr.getSecondOperand());
					return arithExpr.getFirstOperand();
				}
			});
	}

	private final List<Object2DoubleMap<PathExpression>> weights = new ArrayList<Object2DoubleMap<PathExpression>>();

	private RuleManager fusionRules = new RuleManager(), updateRules = new RuleManager(), weightRules = new RuleManager();

	private boolean multipleRecordsPerSource = false;

	private EvaluationExpression defaultValueRule = MergeRule.INSTANCE;

	public EvaluationExpression getDefaultValueRule() {
		return this.defaultValueRule;
	}

	private Object2DoubleMap<PathExpression> getWeightMap(final int inputIndex) {
		CollectionUtil.ensureSize(weights, inputIndex + 1);
		Object2DoubleMap<PathExpression> weightMap = this.weights.get(inputIndex);
		if (weightMap == null) {
			while (this.weights.size() <= inputIndex)
				this.weights.add(null);
			this.weights.set(inputIndex, weightMap = new Object2DoubleOpenHashMap<PathExpression>());
			weightMap.defaultReturnValue(1);
		}
		return weightMap;
	}

	public double getWeights(final int inputIndex, final PathExpression path) {
		return this.getWeightMap(inputIndex).getDouble(path);
	}

	public boolean isMultipleRecordsPerSource() {
		return this.multipleRecordsPerSource;
	}

	public void addFusionRule(EvaluationExpression rule, List<EvaluationExpression> target) {
		this.fusionRules.addRule(rule, target);
	}

	public void addFusionRule(EvaluationExpression rule, EvaluationExpression... target) {
		this.fusionRules.addRule(rule, target);
	}

	public void removeFusionRule(EvaluationExpression rule, List<EvaluationExpression> target) {
		this.fusionRules.removeRule(rule, target);
	}

	public void removeFusionRule(EvaluationExpression rule, EvaluationExpression... target) {
		this.fusionRules.removeRule(rule, target);
	}

	public void addUpdateRule(EvaluationExpression rule, List<EvaluationExpression> target) {
		this.updateRules.addRule(rule, target);
	}

	public void addUpdateRule(EvaluationExpression rule, EvaluationExpression... target) {
		this.updateRules.addRule(rule, target);
	}

	public void removeUpdateRule(EvaluationExpression rule, List<EvaluationExpression> target) {
		this.updateRules.removeRule(rule, target);
	}

	public void removeUpdateRule(EvaluationExpression rule, EvaluationExpression... target) {
		this.updateRules.removeRule(rule, target);
	}

	@Property
	@Name(noun = "default")
	public void setDefaultValueRule(final EvaluationExpression defaultValueRule) {
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
		this.fusionRules.parse(ruleExpression, this, FusionRuleFactory);
	}

	public ObjectCreation getFusionExpression() {
		return (ObjectCreation) this.fusionRules.getLastParsedExpression();
	}

	@Property
	@Name(preposition = "with weights")
	public void setWeightExpression(ObjectCreation ruleExpression) {
		this.weightRules.parse(ruleExpression, this, WeightRuleFactory);
		
		for(Entry<PathExpression, EvaluationExpression> rule : weightRules.getRules()) {
			PathExpression path = rule.getKey();
			parseWeightRule(rule.getValue(), ((InputSelection) path.getFragment(0)).getIndex(), path.subPath(1, -1));
		}
	}

	private void parseWeightRule(EvaluationExpression value, int index, PathExpression path) {
		if(value instanceof ArithmeticExpression) {
			setWeight(((NumericNode) ((ConstantExpression) ((ArithmeticExpression) value).getFirstOperand()).getConstant()).getDoubleValue(), index, path);
			parseWeightRule(((ArithmeticExpression) value).getSecondOperand(), index, new PathExpression(path));
		} else
			setWeight(((NumericNode) ((ConstantExpression) value).getConstant()).getDoubleValue(), index, path);
	}

	public ObjectCreation getWeightExpression() {
		return (ObjectCreation) this.weightRules.getLastParsedExpression();
	}

	@Property
	@Name(verb = "update")
	public void setUpdateExpression(ObjectCreation ruleExpression) {
		this.updateRules.parse(ruleExpression, this, UpdateRuleFactory);
	}

	public ObjectCreation getUpdateExpression() {
		return (ObjectCreation) this.updateRules.getLastParsedExpression();
	}

	@Override
	public SopremoModule asElementaryOperators() {
		return SopremoModule.valueOf("fusion", new ValueFusion().withFusionRules(this.fusionRules));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.defaultValueRule.hashCode();
		result = prime * result + this.fusionRules.hashCode();
		result = prime * result + (this.multipleRecordsPerSource ? 1231 : 1237);
		result = prime * result + this.updateRules.hashCode();
		result = prime * result + this.weights.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		Fusion other = (Fusion) obj;
		return this.defaultValueRule.equals(other.defaultValueRule)
			&& this.fusionRules.equals(other.fusionRules)
			&& this.multipleRecordsPerSource == other.multipleRecordsPerSource
			&& this.updateRules.equals(other.updateRules)
			&& this.weights.equals(other.weights);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.Operator#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		super.toString(builder);
		builder.append(" ").append(this.fusionRules).
			append(" default ").append(this.defaultValueRule).
			append(" with weights ").append(this.weights).
			append(" update ").append(this.updateRules);
	}

	public static class ValueFusion extends ElementaryOperator<ValueFusion> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1419963669395136640L;

		private RuleManager fusionRules = new RuleManager();

		public void setFusionRules(RuleManager fusionRules) {
			if (fusionRules == null)
				throw new NullPointerException("fusionRules must not be null");

			this.fusionRules = fusionRules;
		}

		public RuleManager getFusionRules() {
			return this.fusionRules;
		}

		public ValueFusion withFusionRules(RuleManager fusionRules) {
			this.setFusionRules(fusionRules);
			return this;
		}

		public static class Implementation extends
				SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {
			private RuleManager fusionRules = new RuleManager();

			private List<Object2DoubleMap<PathExpression>> weights;

			private FusionContext context;

			private boolean multipleRecordsPerSource;

			private ConflictResolution defaultValueRule;

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
				for (EvaluationExpression fusionRule : this.fusionRules.get(currentPath)) {
					this.context.setWeights(weights);
					JsonNode resolvedNode = fusionRule.evaluate(unresolved, this.context);
					if (resolvedNode instanceof UnresolvedNodes)
						unresolved = (UnresolvedNodes) resolvedNode;
					else
						return resolvedNode;
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

			private JsonNode fuseObjects(final JsonNode[] values, final double[] weights,
					final PathExpression currentPath,
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

					JsonNode[] contextArray = this.contextNodes.toArray(new JsonNode[this.contextNodes.size()]);
					this.context.setContextNodes(contextArray);
					final double[] initialWeights = new double[this.contextNodes.size()];
					for (int index = 0; index < initialWeights.length; index++)
						initialWeights[index] = this.getWeight(index, new PathExpression());
					out.collect(key, this.fuse(contextArray, initialWeights, new PathExpression()));
				} catch (final UnresolvableEvaluationException e) {
					// do not emit invalid record
				}
			}
		}
	}

}
