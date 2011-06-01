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
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.Comparison;
import eu.stratosphere.sopremo.expressions.Condition;
import eu.stratosphere.sopremo.expressions.Condition.Combination;
import eu.stratosphere.sopremo.expressions.ConditionalOperator;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression.Quantor;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.Path;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.pact.SopremoCross;
import eu.stratosphere.sopremo.pact.SopremoMatch;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.util.dag.SubGraph;

public class Join extends ConditionalOperator {
	private BitSet outerJoinFlag = new BitSet();

	public Join(EvaluableExpression transformation, Condition condition, JsonStream... inputs) {
		super(transformation, condition, inputs);
	}

	public Join(EvaluableExpression transformation, Condition condition, List<? extends JsonStream> inputs) {
		super(transformation, condition, inputs);
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		if (this.getInputOperators().size() != 2)
			throw new UnsupportedOperationException();

		PactModule module = new PactModule(this.getInputOperators().size(), 1);

		Condition condition = this.getCondition();
		if (condition.getCombination() != Combination.AND)
			throw new UnsupportedOperationException();
		for (BooleanExpression expression : condition.getExpressions()) {

			DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> join;
			if (expression instanceof Comparison)
				join = this.joinFromComparison(module, (Comparison) expression, context);
			else if (expression instanceof ElementInSetExpression)
				join = this.joinFromElementExpression(module, (ElementInSetExpression) expression, context);
			else
				throw new UnsupportedOperationException();

			SopremoUtil.setTransformationAndContext(join.getStubParameters(), this.getTransformation(), context);
		}

		return module;
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

	private DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> joinFromComparison(
			PactModule module, Comparison comparison, EvaluationContext context) {
		Contract in1 = SopremoUtil.addKeyExtraction(module, comparison.getExpr1(), context);
		Contract in2 = SopremoUtil.addKeyExtraction(module, comparison.getExpr2(), context);

		// select strategy
		DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> join = null;
		switch (comparison.getBinaryOperator()) {
		case EQUAL:
			if (!this.outerJoinFlag.isEmpty()) {
				boolean leftOuter = this.outerJoinFlag.get(SopremoUtil.getInputIndex((Path) comparison.getExpr1()));
				boolean rightOuter = this.outerJoinFlag.get(SopremoUtil.getInputIndex((Path) comparison.getExpr2()));
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
			SopremoUtil.setObject(join.getStubParameters(), "comparison", comparison);
			break;

		}

		module.getOutput(0).setInput(join);
		join.setFirstInput(in1);
		join.setSecondInput(in2);
		return join;
	}

	private DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> joinFromElementExpression(
		PactModule module, ElementInSetExpression comparison, EvaluationContext context) {
		Contract in1 = SopremoUtil.addKeyExtraction(module, (Path) comparison.getElementExpr(), context);
		Contract in2 = SopremoUtil.addKeyExtraction(module, (Path) comparison.getSetExpr(), context);

		// select strategy
		DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> join = null;
		if (comparison.getQuantor() == Quantor.EXISTS_NOT_IN)
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
		private Comparison comparison;

		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
			this.comparison = SopremoUtil.getObject(parameters, "comparison", Comparison.class);
		}

		@Override
		public void cross(PactJsonObject.Key key1, PactJsonObject value1, PactJsonObject.Key key2,
				PactJsonObject value2,
				Collector<Key, PactJsonObject> out) {
			JsonNode inputPair = JsonUtil.asArray(value1.getValue(), value2.getValue());
			if (this.comparison.evaluate(inputPair, this.getContext()) == BooleanNode.TRUE)
				out.collect(PactNull.getInstance(),
					new PactJsonObject(this.getTransformation().evaluate(inputPair, this.getContext())));
		}
	}

}
