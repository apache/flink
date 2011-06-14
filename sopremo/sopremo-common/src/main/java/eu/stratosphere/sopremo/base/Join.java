package eu.stratosphere.sopremo.base;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.CompactArrayNode;
import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.ArrayMerger;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ConditionalExpression;
import eu.stratosphere.sopremo.expressions.ConditionalExpression.Combination;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression.Quantor;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.pact.KeyExtractionStub;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.pact.SopremoCross;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoMatch;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class Join extends ConditionalOperator {
	private BitSet outerJoinFlag = new BitSet();

	public Join(EvaluableExpression transformation, ConditionalExpression condition, JsonStream... inputs) {
		super(transformation, condition, inputs);
	}

	public Join(EvaluableExpression transformation, ConditionalExpression condition, List<? extends JsonStream> inputs) {
		super(transformation, condition, inputs);
	}

	abstract class TwoSourceJoin {
		int leftIndex, rightIndex;

		public TwoSourceJoin(EvaluableExpression expr1, EvaluableExpression expr2) {
			this.leftIndex = SopremoUtil.getInputIndex(expr1);
			this.rightIndex = SopremoUtil.getInputIndex(expr2);
		}

		public abstract DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> createJoinContract(
				PactModule module, EvaluationContext context, Contract left, Contract right);

	}

	class ComparisonJoin extends TwoSourceJoin {
		ComparativeExpression comparison;

		public ComparisonJoin(ComparativeExpression comparison) {
			super(comparison.getExpr1(), comparison.getExpr2());
			this.comparison = comparison;
		}

		@Override
		public DualInputContract<eu.stratosphere.sopremo.pact.PactJsonObject.Key, PactJsonObject, eu.stratosphere.sopremo.pact.PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> createJoinContract(
				PactModule module, EvaluationContext context, Contract left, Contract right) {
			MapContract<PactNull, PactJsonObject, Key, PactJsonObject> in1 =
				new MapContract<PactNull, PactJsonObject, Key, PactJsonObject>(KeyExtractionStub.class);
			SopremoUtil.setTransformationAndContext(in1.getStubParameters(), this.comparison.getExpr1(), context);
			in1.setInput(left);
			MapContract<PactNull, PactJsonObject, Key, PactJsonObject> in2 =
				new MapContract<PactNull, PactJsonObject, Key, PactJsonObject>(KeyExtractionStub.class);
			SopremoUtil.setTransformationAndContext(in2.getStubParameters(), this.comparison.getExpr2(), context);
			in2.setInput(right);

			// select strategy
			DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> join = null;
			switch (this.comparison.getBinaryOperator()) {
			case EQUAL:
				if (!Join.this.outerJoinFlag.isEmpty()) {
					boolean leftOuter = Join.this.outerJoinFlag.get(SopremoUtil.getInputIndex((PathExpression) this.comparison
						.getExpr1()));
					boolean rightOuter = Join.this.outerJoinFlag
						.get(SopremoUtil.getInputIndex((PathExpression) this.comparison.getExpr2()));
					join = new CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject>(
							OuterJoinStub.class);
					join.getStubParameters().setBoolean("leftOuter", leftOuter);
					join.getStubParameters().setBoolean("rightOuter", rightOuter);
					break;
				}

				join = new MatchContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject>(
						InnerJoinStub.class);
				break;
			default:
				join = new CrossContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, Key, PactJsonObject>(
						ThetaJoinStub.class);
				SopremoUtil.serialize(join.getStubParameters(), "comparison", this.comparison);
				break;

			}

			module.getOutput(0).setInput(join);
			join.setFirstInput(in1);
			join.setSecondInput(in2);
			return join;
		}
	}

	class ElementInSetJoin extends TwoSourceJoin {
		ElementInSetExpression elementInSetExpression;

		public ElementInSetJoin(ElementInSetExpression elementInSetExpression) {
			super(elementInSetExpression.getElementExpr(), elementInSetExpression.getSetExpr());
			this.elementInSetExpression = elementInSetExpression;
		}

		@Override
		public DualInputContract<eu.stratosphere.sopremo.pact.PactJsonObject.Key, PactJsonObject, eu.stratosphere.sopremo.pact.PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> createJoinContract(
				PactModule module, EvaluationContext context, Contract left, Contract right) {
			MapContract<PactNull, PactJsonObject, Key, PactJsonObject> in1 =
				new MapContract<PactNull, PactJsonObject, Key, PactJsonObject>(KeyExtractionStub.class);
			SopremoUtil.setTransformationAndContext(in1.getStubParameters(),
				this.elementInSetExpression.getElementExpr(), context);
			in1.setInput(left);
			MapContract<PactNull, PactJsonObject, Key, PactJsonObject> in2 =
				new MapContract<PactNull, PactJsonObject, Key, PactJsonObject>(KeyExtractionStub.class);
			SopremoUtil.setTransformationAndContext(in2.getStubParameters(), this.elementInSetExpression.getSetExpr(),
				context);
			in2.setInput(right);

			// select strategy
			DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> join = null;
			if (this.elementInSetExpression.getQuantor() == Quantor.EXISTS_NOT_IN)
				join = new CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject>(
					AntiJoinStub.class);
			else
				join = new CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject>(
						SemiJoinStub.class);

			module.getOutput(0).setInput(join);
			join.setFirstInput(in1);
			join.setSecondInput(in2);
			return join;
		}
	}

	public static class ArrayWrapper extends SopremoMap<PactNull, PactJsonObject, PactNull, PactJsonObject> {
		private int arraySize, arrayIndex;

		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
			this.arraySize = parameters.getInteger("arraySize", 0);
			this.arrayIndex = parameters.getInteger("arrayIndex", 0);
		}

		@Override
		public void map(PactNull key, PactJsonObject value, Collector<PactNull, PactJsonObject> out) {
			JsonNode[] array = new JsonNode[this.arraySize];
			array[this.arrayIndex] = value.getValue();
			out.collect(key, new PactJsonObject(new CompactArrayNode(array)));
		}
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		PactModule module = new PactModule(this.getInputOperators().size(), 1);

		ConditionalExpression condition = this.getCondition();
		if (condition.getCombination() != Combination.AND)
			throw new UnsupportedOperationException();

		List<TwoSourceJoin> joinOrder = this.getInitialJoinOrder(condition);
		ArrayList<Contract> inputs = this.wrapInputsWithArray(context, module);

		DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> join = null;
		for (TwoSourceJoin twoSourceJoin : joinOrder) {
			join = twoSourceJoin.createJoinContract(module, context, inputs.get(twoSourceJoin.leftIndex),
				inputs.get(twoSourceJoin.rightIndex));
			inputs.set(twoSourceJoin.leftIndex, join);
			inputs.set(twoSourceJoin.rightIndex, join);

			SopremoUtil.setTransformationAndContext(join.getStubParameters(), new ArrayMerger(), context);
		}

		MapContract<PactNull, PactJsonObject, Key, PactJsonObject> projectionMap = new MapContract<PactNull, PactJsonObject, Key, PactJsonObject>(
				Projection.ProjectionStub.class);
		module.getOutput(0).setInput(projectionMap);
		projectionMap.setInput(join);
		SopremoUtil.setTransformationAndContext(projectionMap.getStubParameters(), this.getTransformation(), context);

		return module;
	}

	private ArrayList<Contract> wrapInputsWithArray(EvaluationContext context, PactModule module) {
		ArrayList<Contract> inputs = new ArrayList<Contract>();
		List<Output> rawInputs = this.getInputs();
		for (int index = 0; index < rawInputs.size(); index++) {
			MapContract<PactNull, PactJsonObject, PactNull, PactJsonObject> arrayWrapMap = new MapContract<PactNull, PactJsonObject, PactNull, PactJsonObject>(
				ArrayWrapper.class);
			inputs.add(arrayWrapMap);
			arrayWrapMap.setInput(module.getInput(index));
			SopremoUtil
				.setTransformationAndContext(arrayWrapMap.getStubParameters(), this.getTransformation(), context);
			arrayWrapMap.getStubParameters().setInteger("arraySize", rawInputs.size());
			arrayWrapMap.getStubParameters().setInteger("arrayIndex", index);
		}
		return inputs;
	}

	private List<TwoSourceJoin> getInitialJoinOrder(ConditionalExpression condition) {
		List<TwoSourceJoin> joins = new ArrayList<TwoSourceJoin>();
		for (BooleanExpression expression : condition.getExpressions())
			if (expression instanceof ComparativeExpression)
				joins.add(new ComparisonJoin((ComparativeExpression) expression));
			else if (expression instanceof ElementInSetExpression)
				joins.add(new ElementInSetJoin((ElementInSetExpression) expression));
			else
				throw new UnsupportedOperationException();

		// TODO: add some kind of optimizations
		return joins;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		Join other = (Join) obj;
		return this.outerJoinFlag.equals(other.outerJoinFlag);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.outerJoinFlag.hashCode();
		return result;
	}

	public boolean isOuterJoin(JsonStream input) {
		int index = this.getInputs().indexOf(input.getSource());
		if (index == -1)
			throw new IllegalArgumentException();
		return this.outerJoinFlag.get(index);
	}

	public void setOuterJoin(JsonStream... inputs) {
		for (JsonStream input : inputs) {
			int index = this.getInputs().indexOf(input.getSource());
			if (index == -1)
				throw new IllegalArgumentException();
			this.outerJoinFlag.set(index);
		}
	}

	public Join withOuterJoin(JsonStream... inputs) {
		this.setOuterJoin(inputs);
		return this;
	}

	public static class AntiJoinStub extends
			SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
		@Override
		public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<Key, PactJsonObject> out) {
			if (!values2.hasNext())
				while (values1.hasNext()) {
					JsonNode result = this.getTransformation().evaluate(JsonUtil.asArray(values1.next().getValue()),
						this.getContext());
					out.collect(PactNull.getInstance(), new PactJsonObject(result));
				}
		}
	}

	public static class InnerJoinStub extends
			SopremoMatch<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
		@Override
		public void match(PactJsonObject.Key key, PactJsonObject value1, PactJsonObject value2,
				Collector<Key, PactJsonObject> out) {
			JsonNode result = this.getTransformation()
				.evaluate(JsonUtil.asArray(value1.getValue(), value2.getValue()), this.getContext());
			out.collect(PactNull.getInstance(), new PactJsonObject(result));
		}
	}

	public static class OuterJoinStub extends
			SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
		private boolean leftOuter, rightOuter;

		@Override
		public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<Key, PactJsonObject> out) {

			if (!values1.hasNext()) {
				// special case: no items from first source
				// emit all values of the second source
				if (this.rightOuter)
					while (values2.hasNext())
						out.collect(
							PactNull.getInstance(),
							new PactJsonObject(this.getTransformation()
								.evaluate(
									JsonUtil.asArray(NullNode.getInstance(), values2.next().getValue()),
									this.getContext())));
				return;
			}

			if (!values2.hasNext()) {
				// special case: no items from second source
				// emit all values of the first source
				if (this.leftOuter)
					while (values1.hasNext())
						out.collect(
							PactNull.getInstance(),
							new PactJsonObject(this.getTransformation()
								.evaluate(
									JsonUtil.asArray(values1.next().getValue(), NullNode.getInstance()),
									this.getContext())));
				return;
			}

			// TODO: use resettable iterator to avoid OOM
			ArrayList<JsonNode> firstSourceNodes = new ArrayList<JsonNode>();
			while (values1.hasNext())
				firstSourceNodes.add(values1.next().getValue());

			while (values2.hasNext()) {
				JsonNode secondSourceNode = values2.next().getValue();
				for (JsonNode firstSourceNode : firstSourceNodes)
					out.collect(
						PactNull.getInstance(),
						new PactJsonObject(this.getTransformation().evaluate(JsonUtil.asArray(firstSourceNode,
							secondSourceNode), this.getContext())));
			}
		}

		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
			this.leftOuter = parameters.getBoolean("leftOuter", false);
			this.rightOuter = parameters.getBoolean("rightOuter", false);
		}
	}

	public static class SemiJoinStub extends
			SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
		@Override
		public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<Key, PactJsonObject> out) {
			if (values2.hasNext())
				while (values1.hasNext()) {
					JsonNode result = this.getTransformation().evaluate(JsonUtil.asArray(values1.next().getValue()),
						this.getContext());
					out.collect(PactNull.getInstance(), new PactJsonObject(result));
				}
		}
	}

	public static class ThetaJoinStub extends
			SopremoCross<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> {
		private ComparativeExpression comparison;

		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
			this.comparison = SopremoUtil.deserialize(parameters, "comparison", ComparativeExpression.class);
		}

		@Override
		public void cross(PactJsonObject.Key key1, PactJsonObject value1, PactJsonObject.Key key2,
				PactJsonObject value2,
				Collector<Key, PactJsonObject> out) {
			if (this.comparison.evaluate(JsonUtil.asArray(value1.getValue().get(0), value2.getValue().get(1)),
				this.getContext()) == BooleanNode.TRUE)
				out.collect(PactNull.getInstance(),
					new PactJsonObject(this.getTransformation().evaluate(
						JsonUtil.asArray(value1.getValue(), value2.getValue()), this.getContext())));
		}
	}

}
