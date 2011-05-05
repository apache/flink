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
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.BooleanExpression;
import eu.stratosphere.sopremo.Comparison;
import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Condition.Combination;
import eu.stratosphere.sopremo.ElementExpression;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.Input;
import eu.stratosphere.sopremo.expressions.Path;
import eu.stratosphere.sopremo.expressions.Transformation;

public class Join extends ConditionalOperator {
	private BitSet outerJoinFlag = new BitSet();

	public Join(Transformation transformation, Condition condition, Operator... inputs) {
		super(transformation, condition, inputs);
	}

	public Join(Transformation transformation, Condition condition, List<Operator> inputs) {
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
			MatchStub<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> {
		private Transformation transformation;

		@Override
		public void configure(Configuration parameters) {
			this.transformation = getTransformation(parameters, "transformation");
		}

		@Override
		public void match(PactJsonObject.Key key, PactJsonObject value1, PactJsonObject value2,
				Collector<PactNull, PactJsonObject> out) {
			out.collect(PactNull.getInstance(),
				new PactJsonObject(this.transformation.evaluate(value1.getValue(), value2.getValue())));
		}
	}

	public static class AntiJoinStub extends
			CoGroupStub<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> {
		private Transformation transformation;

		@Override
		public void configure(Configuration parameters) {
			this.transformation = getTransformation(parameters, "transformation");
		}

		@Override
		public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<PactNull, PactJsonObject> out) {
			if (!values2.hasNext())
				while (values1.hasNext())
					out.collect(PactNull.getInstance(),
						new PactJsonObject(transformation.evaluate(values1.next().getValue())));
		}
	}

	public static class SemiJoinStub extends
			CoGroupStub<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> {
		private Transformation transformation;

		@Override
		public void configure(Configuration parameters) {
			this.transformation = getTransformation(parameters, "transformation");
		}

		@Override
		public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<PactNull, PactJsonObject> out) {
			if (values2.hasNext())
				while (values1.hasNext())
					out.collect(PactNull.getInstance(),
						new PactJsonObject(transformation.evaluate(values1.next().getValue())));
		}
	}

	public static class ThetaJoinStub extends
			CrossStub<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactNull, PactJsonObject> {
		private Transformation transformation;

		private Comparison comparison;

		@Override
		public void configure(Configuration parameters) {
			this.transformation = getTransformation(parameters, "transformation");
			this.comparison = getObject(parameters, "comparison", Comparison.class);
		}

		@Override
		public void cross(PactJsonObject.Key key1, PactJsonObject value1, PactJsonObject.Key key2,
				PactJsonObject value2, Collector<PactNull, PactJsonObject> out) {
			if (this.comparison.evaluate(value1.getValue(), value2.getValue()) == BooleanNode.TRUE)
				out.collect(PactNull.getInstance(),
					new PactJsonObject(this.transformation.evaluate(value1.getValue(), value2.getValue())));
		}
	}

	public static class OuterJoinStub extends
			CoGroupStub<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> {
		private Transformation transformation;

		private boolean leftOuter, rightOuter;

		@Override
		public void configure(Configuration parameters) {
			this.transformation = getTransformation(parameters, "transformation");
			this.leftOuter = parameters.getBoolean("leftOuter", false);
			this.rightOuter = parameters.getBoolean("rightOuter", false);
		}

		@Override
		public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<PactNull, PactJsonObject> out) {

			if (!values1.hasNext()) {
				// special case: no items from first source
				// emit all values of the second source
				if (this.leftOuter)
					while (values2.hasNext())
						out.collect(PactNull.getInstance(),
							new PactJsonObject(this.transformation.evaluate(NullNode.getInstance(), values2.next()
								.getValue())));
				return;
			}

			if (!values2.hasNext()) {
				// special case: no items from second source
				// emit all values of the first source
				if (this.rightOuter)
					while (values1.hasNext())
						out.collect(PactNull.getInstance(),
							new PactJsonObject(this.transformation.evaluate(NullNode.getInstance(), values1.next()
								.getValue())));
				return;
			}

			// TODO: use resettable iterator to avoid OOM
			ArrayList<JsonNode> firstSourceNodes = new ArrayList<JsonNode>();
			while (values1.hasNext())
				firstSourceNodes.add((values1.next()).getValue());

			while (values2.hasNext()) {
				JsonNode secondSourceNode = values2.next().getValue();
				for (JsonNode firstSourceNode : firstSourceNodes)
					out.collect(PactNull.getInstance(),
						new PactJsonObject(this.transformation.evaluate(firstSourceNode, secondSourceNode)));
			}
		}
	}

	@Override
	public PactModule asPactModule() {
		if (this.getInputOperators().size() != 2)
			throw new UnsupportedOperationException();

		PactModule module = new PactModule(this.getInputOperators().size(), 1);

		Condition condition = this.getCondition();
		if (condition.getCombination() != Combination.AND)
			throw new UnsupportedOperationException();
		for (BooleanExpression expression : condition.getExpressions()) {

			DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactNull, PactJsonObject> join;
			if (expression instanceof Comparison)
				join = joinFromComparison(module, (Comparison) expression);
			else if (expression instanceof ElementExpression)
				join = joinFromElementExpression(module, (ElementExpression) expression);
			else
				throw new UnsupportedOperationException();

			setTransformation(join.getStubParameters(), "transformation", this.getTransformation());
		}

		return module;
	}

	private DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactNull, PactJsonObject> joinFromComparison(
			PactModule module, Comparison comparison) {
		Contract in1 = this.addKeyExtraction(module, (Path) comparison.getExpr1());
		Contract in2 = this.addKeyExtraction(module, (Path) comparison.getExpr2());

		// select strategy
		DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactNull, PactJsonObject> join = null;
		switch (comparison.getBinaryOperator()) {
		case EQUAL:
			if (!this.outerJoinFlag.isEmpty()) {
				boolean leftOuter = this.outerJoinFlag.get(this.getInputIndex((Path) comparison.getExpr1()));
				boolean rightOuter = this.outerJoinFlag.get(this.getInputIndex((Path) comparison.getExpr2()));
				join = new CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject>(
						OuterJoinStub.class);
				join.getStubParameters().setBoolean("leftOuter", leftOuter);
				join.getStubParameters().setBoolean("rightOuter", rightOuter);
				break;
			}

			join = new MatchContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject>(
					InnerJoinStub.class);
			break;
		default:
			join = new CrossContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactNull, PactJsonObject>(
					ThetaJoinStub.class);
			setObject(join.getStubParameters(), "comparison", comparison);
			break;

		}

		module.getOutput(0).setInput(join);
		join.setFirstInput(in1);
		join.setSecondInput(in2);
		return join;
	}

	private DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactNull, PactJsonObject> joinFromElementExpression(
			PactModule module, ElementExpression comparison) {
		Contract in1 = this.addKeyExtraction(module, (Path) comparison.getElementExpr());
		Contract in2 = this.addKeyExtraction(module, (Path) comparison.getSetExpr());

		// select strategy
		DualInputContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactNull, PactJsonObject> join = null;
		if (comparison.isNotIn())
			join = new CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject>(
				AntiJoinStub.class);
		else
			join = new CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject>(
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
