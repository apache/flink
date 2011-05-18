package eu.stratosphere.sopremo.operator;

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
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtils;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.Comparison;
import eu.stratosphere.sopremo.expressions.Condition;
import eu.stratosphere.sopremo.expressions.Condition.Combination;
import eu.stratosphere.sopremo.expressions.ElementExpression;
import eu.stratosphere.sopremo.expressions.ElementExpression.Quantor;
import eu.stratosphere.sopremo.expressions.Path;

public class Join extends ConditionalOperator {
	private BitSet outerJoinFlag = new BitSet();

	public Join(Evaluable transformation, Condition condition, Operator... inputs) {
		super(transformation, condition, inputs);
	}

	public Join(Evaluable transformation, Condition condition, List<Operator> inputs) {
		super(transformation, condition, inputs);
	}

	public Join withOuterJoin(Operator... operators) {
		setOuterJoin(operators);
		return this;
	}

	public void setOuterJoin(Operator... operators) {
		for (Operator operator : operators) {
			int index = this.getInputOperators().indexOf(operator);
			if (index == -1)
				throw new IllegalArgumentException();
			this.outerJoinFlag.set(index);
		}
	}

	public static class InnerJoinStub extends
			SopremoMatch<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
		@Override
		public void match(PactJsonObject.Key key, PactJsonObject value1, PactJsonObject value2,
				Collector<Key, PactJsonObject> out) {
			JsonNode result = this.getTransformation()
				.evaluate(JsonUtils.asArray(value1.getValue(), value2.getValue()), getContext());
			out.collect(PactNull.getInstance(), new PactJsonObject(result));
		}
	}

	public static class AntiJoinStub extends
			SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
		@Override
		public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<Key, PactJsonObject> out) {
			if (!values2.hasNext())
				while (values1.hasNext()) {
					JsonNode result = this.getTransformation().evaluate(JsonUtils.asArray(values1.next().getValue()), getContext());
					out.collect(PactNull.getInstance(), new PactJsonObject(result));
				}
		}
	}

	public static class SemiJoinStub extends
			SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
		@Override
		public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<Key, PactJsonObject> out) {
			if (values2.hasNext())
				while (values1.hasNext()) {
					JsonNode result = this.getTransformation().evaluate(JsonUtils.asArray(values1.next().getValue()), getContext());
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
			this.comparison = PactUtil.getObject(parameters, "comparison", Comparison.class);
		}

		@Override
		public void cross(PactJsonObject.Key key1, PactJsonObject value1, PactJsonObject.Key key2, PactJsonObject value2,
				Collector<Key, PactJsonObject> out) {
			JsonNode inputPair = JsonUtils.asArray(value1.getValue(), value2.getValue());
			if (this.comparison.evaluate(inputPair, getContext()) == BooleanNode.TRUE)
				out.collect(PactNull.getInstance(), new PactJsonObject(this.getTransformation().evaluate(inputPair, getContext())));
		}
	}

	public static class OuterJoinStub extends SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
		private boolean leftOuter, rightOuter;

		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
			this.leftOuter = parameters.getBoolean("leftOuter", false);
			this.rightOuter = parameters.getBoolean("rightOuter", false);
		}

		@Override
		public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<Key, PactJsonObject> out) {

			if (!values1.hasNext()) {
				// special case: no items from first source
				// emit all values of the second source
				if (this.rightOuter)
					while (values2.hasNext())
						out.collect(PactNull.getInstance(),
							new PactJsonObject(this.getTransformation().evaluate(
								JsonUtils.asArray(NullNode.getInstance(), values2.next().getValue()), getContext())));
				return;
			}

			if (!values2.hasNext()) {
				// special case: no items from second source
				// emit all values of the first source
				if (this.leftOuter)
					while (values1.hasNext())
						out.collect(PactNull.getInstance(),
							new PactJsonObject(this.getTransformation().evaluate(
								JsonUtils.asArray(values1.next().getValue(), NullNode.getInstance()), getContext())));
				return;
			}

			// TODO: use resettable iterator to avoid OOM
			ArrayList<JsonNode> firstSourceNodes = new ArrayList<JsonNode>();
			while (values1.hasNext())
				firstSourceNodes.add((values1.next()).getValue());

			while (values2.hasNext()) {
				JsonNode secondSourceNode = values2.next().getValue();
				for (JsonNode firstSourceNode : firstSourceNodes)
					out.collect(
						PactNull.getInstance(),
						new PactJsonObject(this.getTransformation().evaluate(JsonUtils.asArray(firstSourceNode,
							secondSourceNode), getContext())));
			}
		}
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
				join = joinFromComparison(module, (Comparison) expression, context);
			else if (expression instanceof ElementExpression)
				join = joinFromElementExpression(module, (ElementExpression) expression, context);
			else
				throw new UnsupportedOperationException();
			
			PactUtil.setTransformationAndContext(join.getStubParameters(), this.getEvaluableExpression(), context);
		}

		return module;
	}

	private DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> joinFromComparison(
			PactModule module, Comparison comparison, EvaluationContext context) {
		Contract in1 = PactUtil.addKeyExtraction(module, (Path) comparison.getExpr1(), context);
		Contract in2 = PactUtil.addKeyExtraction(module, (Path) comparison.getExpr2(), context);

		// select strategy
		DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> join = null;
		switch (comparison.getBinaryOperator()) {
		case EQUAL:
			if (!this.outerJoinFlag.isEmpty()) {
				boolean leftOuter = this.outerJoinFlag.get(PactUtil.getInputIndex((Path) comparison.getExpr1()));
				boolean rightOuter = this.outerJoinFlag.get(PactUtil.getInputIndex((Path) comparison.getExpr2()));
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
			PactUtil.setObject(join.getStubParameters(), "comparison", comparison);
			break;

		}

		module.getOutput(0).setInput(join);
		join.setFirstInput(in1);
		join.setSecondInput(in2);
		return join;
	}

	private DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> joinFromElementExpression(
			PactModule module, ElementExpression comparison, EvaluationContext context) {
		Contract in1 = PactUtil.addKeyExtraction(module, (Path) comparison.getElementExpr(), context);
		Contract in2 = PactUtil.addKeyExtraction(module, (Path) comparison.getSetExpr(), context);

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

	public boolean isOuterJoin(Operator operator) {
		int index = this.getInputOperators().indexOf(operator);
		if (index == -1)
			throw new IllegalArgumentException();
		return this.outerJoinFlag.get(index);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.outerJoinFlag.hashCode();
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
		Join other = (Join) obj;
		return this.outerJoinFlag.equals(other.outerJoinFlag);
	}

}
