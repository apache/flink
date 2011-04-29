package eu.stratosphere.sopremo.operator;

import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.Transformation;
import eu.stratosphere.sopremo.operator.Projection.ProjectionStub;

public class Join extends ConditionalOperator {
	private BitSet outerJoinFlag = new BitSet();

	public Join(Transformation transformation, Condition condition, Operator... inputs) {
		super(transformation, condition, inputs);
	}

	public Join(Transformation transformation, Condition condition, List<Operator> inputs) {
		super(transformation, condition, inputs);
	}

	public Join withOuterJoin(Operator operator) {
		int index = getInputOperators().indexOf(operator);
		if (index == -1)
			throw new IllegalArgumentException();
		outerJoinFlag.set(index);
		return this;
	}

	public static class InnerJoinStub extends MatchStub<Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> {
		@Override
		public void match(Key key, PactJsonObject value1, PactJsonObject value2,
				Collector<PactNull, PactJsonObject> out) {
		}
	}

	public static class AntiJoinStub extends CoGroupStub<Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> {
		@Override
		public void coGroup(Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<PactNull, PactJsonObject> out) {
		}
	}

	public static class OuterJoinStub extends
			CoGroupStub<Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> {
		@Override
		public void coGroup(Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<PactNull, PactJsonObject> out) {
		}
	}

	@Override
	public PactModule asPactModule() {
		if (this.getInputOperators().size() != 2)
			throw new UnsupportedOperationException();

		PactModule module = new PactModule(getInputOperators().size(), 1);
		for (int index = 0; index < getInputOperators().size(); index++) {
			// extract keys
		}

		// select strategy
//		MatchContract<Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> joinMatch = new MatchContract<Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject>(
//				EquiJoinStub.class);
//		module.getOutput(0).setInput(joinMatch);
//		joinMatch.setInput(module.getInput(0));
		return module;
	}

	public boolean isOuterJoin(Operator operator) {
		int index = getInputOperators().indexOf(operator);
		if (index == -1)
			throw new IllegalArgumentException();
		return outerJoinFlag.get(index);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + outerJoinFlag.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Join other = (Join) obj;
		return outerJoinFlag.equals(other.outerJoinFlag);
	}

}
