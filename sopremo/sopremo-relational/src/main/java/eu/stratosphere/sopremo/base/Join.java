package eu.stratosphere.sopremo.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.StreamArrayNode;
import eu.stratosphere.sopremo.expressions.AndExpression;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ArrayMerger;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ContainerExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression.Quantor;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ExpressionTag;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.pact.SopremoCross;
import eu.stratosphere.sopremo.pact.SopremoMatch;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class Join extends CompositeOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6428723643579047169L;

	private BooleanExpression condition;

	private EvaluationExpression expression;

	public Join(EvaluationExpression expression, BooleanExpression condition, JsonStream... inputs) {
		super(inputs);
		this.condition = condition;
		this.expression = expression;
	}

	public Join(EvaluationExpression expression, BooleanExpression condition, List<? extends JsonStream> inputs) {
		super(inputs);
		this.condition = condition;
		this.expression = expression;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		return super.equals(obj) && this.condition.equals(((Join) obj).condition)
			&& this.expression.equals(((Join) obj).expression);
	}

	public BooleanExpression getCondition() {
		return this.condition;
	}

	private List<TwoSourceJoin> getInitialJoinOrder(AndExpression condition) {
		List<TwoSourceJoin> joins = new ArrayList<TwoSourceJoin>();
		for (BooleanExpression expression : condition.getExpressions())
			joins.add(getTwoSourceJoinForExpression(expression));

		// TODO: add some kind of optimization
		return joins;
	}

	private TwoSourceJoin getTwoSourceJoinForExpression(BooleanExpression expression) {
		if (expression instanceof ComparativeExpression)
			return new ComparisonJoin(
				this.getInputForExpression(((ComparativeExpression) expression).getExpr1()),
				this.getInputForExpression(((ComparativeExpression) expression).getExpr2()),
				(ComparativeExpression) expression);
		if (expression instanceof ElementInSetExpression)
			return new ElementInSetJoin(this.getInputForExpression(((ElementInSetExpression) expression)
				.getElementExpr()), this.getInputForExpression(((ElementInSetExpression) expression).getSetExpr()),
				(ElementInSetExpression) expression);
		throw new UnsupportedOperationException("expression " + expression + " not supported");
	}

	private Operator getInputForExpression(EvaluationExpression expr1) {
		return this.getInput(((ContainerExpression) expr1).find(InputSelection.class).getIndex()).getOperator();
	}

	@Override
	public SopremoModule asElementaryOperators() {
		List<TwoSourceJoin> joins;
		if (this.condition instanceof AndExpression)
			joins = this.getInitialJoinOrder((AndExpression) this.condition);
		else
			joins = Arrays.asList(getTwoSourceJoinForExpression(condition));

		int numInputs = this.getInputs().size();
		SopremoModule module = new SopremoModule(this.toString(), numInputs, 1);

		List<Operator> inputs = new ArrayList<Operator>();
		for (int index = 0; index < numInputs; index++) {
			EvaluationExpression[] elements = new EvaluationExpression[this.getInputs().size()];
			Arrays.fill(elements, EvaluationExpression.NULL);
			elements[index] = EvaluationExpression.SAME_VALUE;
			inputs.add(new Projection(new ArrayCreation(elements), module.getInput(index)));
		}

		for (TwoSourceJoin twoSourceJoin : joins) {
			List<Output> operatorInputs = twoSourceJoin.getInputs();
			Output[] actualInputs = new Output[2];
			for (int index = 0; index < operatorInputs.size(); index++) {
				int inputIndex = this.getInputs().indexOf(operatorInputs.get(index));
				actualInputs[index] = inputs.get(inputIndex).getSource();
			}
			for (int index = 0; index < operatorInputs.size(); index++) {
				int inputIndex = this.getInputs().indexOf(operatorInputs.get(index));
				inputs.set(inputIndex, twoSourceJoin);
			}
			twoSourceJoin.setInputs(actualInputs);
		}

		module.getOutput(0).setInput(0,
			new Projection(EvaluationExpression.NULL, this.expression, joins.get(joins.size() - 1)));
		return module;
	}

	// @Override
	// public PactModule asPactModule(EvaluationContext context) {
	// PactModule module = new PactModule(this.getInputOperators().size(), 1);
	//
	// ConditionalExpression condition = this.getCondition();
	// if (condition.getCombination() != Combination.AND)
	// throw new UnsupportedOperationException();
	//
	// List<TwoSourceJoin> joinOrder = this.getInitialJoinOrder(condition);
	// List<Contract> inputs = SopremoUtil.wrapInputsWithArray(module, context);
	//
	// DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactJsonObject.Key,
	// PactJsonObject> join = null;
	// for (TwoSourceJoin twoSourceJoin : joinOrder) {
	// join = twoSourceJoin.createJoinContract(module, context, inputs.get(twoSourceJoin.leftIndex),
	// inputs.get(twoSourceJoin.rightIndex));
	// inputs.set(twoSourceJoin.leftIndex, join);
	// inputs.set(twoSourceJoin.rightIndex, join);
	//
	// SopremoUtil.setTransformationAndContext(join.getStubParameters(), new ArrayMerger(), context);
	// }
	//
	// MapContract<PactNull, PactJsonObject, PactJsonObject.Key, PactJsonObject> projectionMap = new
	// MapContract<PactNull, PactJsonObject, PactJsonObject.Key, PactJsonObject>(
	// Projection.ProjectionStub.class);
	// module.getOutput(0).setInput(projectionMap);
	// projectionMap.setInput(join);
	// SopremoUtil.setTransformationAndContext(projectionMap.getStubParameters(), this.getKeyTransformation(), context);
	//
	// return module;
	// }

	@Override
	public int hashCode() {
		final int prime = 37;
		int result = super.hashCode();
		result = prime * result + this.condition.hashCode();
		result = prime * result + this.expression.hashCode();
		return result;
	}

	public static class AntiJoinStub extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 2672827253341673832L;

		public AntiJoinStub(JsonStream left, JsonStream right) {
			super(left, right);
		}

		public static class Implementation extends
				SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			@Override
			protected void coGroup(JsonNode key, StreamArrayNode values1, StreamArrayNode values2, JsonCollector out) {
				if (values2.isEmpty())
					for (JsonNode value : values1)
						out.collect(key, JsonUtil.asArray(value));
			}
		}
	}

	static class ComparisonJoin extends TwoSourceJoin {
		/**
		 * 
		 */
		private static final long serialVersionUID = -3045868252288937108L;

		private ComparativeExpression comparison;

		public ComparisonJoin(JsonStream left, JsonStream right, ComparativeExpression comparison) {
			super(left, right, comparison.getExpr1(), comparison.getExpr2());
			this.comparison = comparison;
		}

		@Override
		public Operator createJoinContract(Operator left, Operator right) {
			switch (this.comparison.getBinaryOperator()) {
			case EQUAL:
				boolean leftOuter = this.getLeftJoinKey().removeTag(ExpressionTag.PRESERVE);
				boolean rightOuter = this.getRightJoinKey().removeTag(ExpressionTag.PRESERVE);
				if (leftOuter || rightOuter)
					return new OuterJoinStub(left, leftOuter, right, rightOuter);

				return new InnerJoinStub(left, right);
			default:
				return new ThetaJoinStub(left, this.comparison, right);
			}
		}
	}

	static class ElementInSetJoin extends TwoSourceJoin {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1650057142925592093L;

		private ElementInSetExpression elementInSetExpression;

		public ElementInSetJoin(JsonStream left, JsonStream right, ElementInSetExpression elementInSetExpression) {
			super(left, right, elementInSetExpression.getElementExpr(), elementInSetExpression.getSetExpr());
			this.elementInSetExpression = elementInSetExpression;
		}

		@Override
		public Operator createJoinContract(Operator left, Operator right) {
			if (this.elementInSetExpression.getQuantor() == Quantor.EXISTS_NOT_IN)
				return new AntiJoinStub(left, right);
			return new SemiJoinStub(left, right);
		}
	}

	public static class InnerJoinStub extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 7145499293300473008L;

		public InnerJoinStub(JsonStream left, JsonStream right) {
			super(left, right);
		}

		public static class Implementation extends
				SopremoMatch<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			@Override
			protected void match(JsonNode key, JsonNode value1, JsonNode value2, JsonCollector out) {
				out.collect(key, JsonUtil.asArray(value1, value2));
			}
		}
	}

	public static class OuterJoinStub extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 317168181417121979L;

		private transient boolean leftOuter, rightOuter;

		public OuterJoinStub(JsonStream left, boolean leftOuter, JsonStream right, boolean rightOuter) {
			super(left, right);
			this.leftOuter = leftOuter;
			this.rightOuter = rightOuter;
		}

		@Override
		protected void configureContract(Contract contract, Configuration configuration, EvaluationContext context) {
			super.configureContract(contract, configuration, context);
			configuration.setBoolean("leftOuter", this.leftOuter);
			configuration.setBoolean("rightOuter", this.rightOuter);
		}

		public static class Implementation extends
				SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			private transient boolean leftOuter, rightOuter;

			@Override
			protected void coGroup(JsonNode key, StreamArrayNode values1, StreamArrayNode values2, JsonCollector out) {
				if (values1.isEmpty()) {
					// special case: no items from first source
					// emit all values of the second source
					if (this.rightOuter)
						for (JsonNode value : values2)
							out.collect(key, JsonUtil.asArray(NullNode.getInstance(), value));
					return;
				}

				if (values2.isEmpty()) {
					// special case: no items from second source
					// emit all values of the first source
					if (this.leftOuter)
						for (JsonNode value : values1)
							out.collect(key, JsonUtil.asArray(value, NullNode.getInstance()));
					return;
				}

				// TODO: use resettable iterator to avoid OOM
				ArrayList<JsonNode> firstSourceNodes = new ArrayList<JsonNode>();
				for (JsonNode value : values1)
					firstSourceNodes.add(value);

				for (JsonNode secondSourceNode : values2)
					for (JsonNode firstSourceNode : firstSourceNodes)
						out.collect(key, JsonUtil.asArray(firstSourceNode, secondSourceNode));
			}

			@Override
			public void configure(Configuration parameters) {
				super.configure(parameters);
				this.leftOuter = parameters.getBoolean("leftOuter", false);
				this.rightOuter = parameters.getBoolean("rightOuter", false);
			}
		}
	}

	public static class SemiJoinStub extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7624313431291367616L;

		public SemiJoinStub(JsonStream left, JsonStream right) {
			super(left, right);
		}

		public static class Implementation extends
				SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			@Override
			protected void coGroup(JsonNode key, StreamArrayNode values1, StreamArrayNode values2, JsonCollector out) {
				if (!values2.isEmpty())
					for (JsonNode value : values1)
						out.collect(key, JsonUtil.asArray(value));
			}
		}
	}

	public static class ThetaJoinStub extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -952011340895859983L;

		private ComparativeExpression comparison;

		public ThetaJoinStub(JsonStream left, ComparativeExpression comparison, JsonStream right) {
			super(left, right);
			this.comparison = comparison;
		}

		@Override
		protected void configureContract(Contract contract, Configuration configuration, EvaluationContext context) {
			super.configureContract(contract, configuration, context);
			SopremoUtil.serialize(configuration, "comparison", this.comparison);
		}

		public static class Implementation
				extends
				SopremoCross<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			private ComparativeExpression comparison;

			@Override
			public void configure(Configuration parameters) {
				super.configure(parameters);
				this.comparison = SopremoUtil.deserialize(parameters, "comparison", ComparativeExpression.class);
			}

			@Override
			protected void cross(JsonNode key1, JsonNode value1, JsonNode key2, JsonNode value2, JsonCollector out) {
				if (this.comparison.evaluate(JsonUtil.asArray(value1.get(0), value2.get(1)), this.getContext()) == BooleanNode.TRUE)
					out.collect(JsonUtil.asArray(key1, key2), JsonUtil.asArray(value1, value2));
			}
		}
	}

	static abstract class TwoSourceJoin extends CompositeOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4192583790586928743L;

		private EvaluationExpression leftJoinKey, rightJoinKey;

		public TwoSourceJoin(JsonStream left, JsonStream right, EvaluationExpression leftJoinKey,
				EvaluationExpression rightJoinKey) {
			super(left, right);
			this.leftJoinKey = leftJoinKey;
			this.rightJoinKey = rightJoinKey;
		}

		@Override
		public SopremoModule asElementaryOperators() {
			SopremoModule sopremoModule = new SopremoModule(this.toString(), 2, 1);

			Projection leftProjection = new Projection(this.leftJoinKey, EvaluationExpression.SAME_VALUE,
				sopremoModule.getInput(0));
			Projection rightProjection = new Projection(this.rightJoinKey, EvaluationExpression.SAME_VALUE,
				sopremoModule.getInput(1));
			Operator joinAlgorithm = this.createJoinContract(leftProjection, rightProjection);
			sopremoModule.getOutput(0).setInputs(new Projection(new ArrayMerger(), joinAlgorithm));
			return sopremoModule;
		}

		public EvaluationExpression getLeftJoinKey() {
			return this.leftJoinKey;
		}

		public EvaluationExpression getRightJoinKey() {
			return this.rightJoinKey;
		}

		public abstract Operator createJoinContract(Operator left, Operator right);

	}

}
