package eu.stratosphere.sopremo.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.Path;
import eu.stratosphere.sopremo.expressions.Transformation;

public class Aggregation extends Operator {
	public final static List<Path> NO_GROUPING = new ArrayList<Path>();

	private List<Path> groupings;

	public Aggregation(Transformation transformation, List<Path> grouping, Operator... inputs) {
		super(transformation, inputs);
		if (grouping == null)
			throw new NullPointerException();
		this.groupings = grouping;
	}

	public Aggregation(Transformation transformation, List<Path> grouping, List<Operator> inputs) {
		super(transformation, inputs);
		if (grouping == null)
			throw new NullPointerException();
		this.groupings = grouping;
	}

	public static class AggregationStub extends
			ReduceStub<Key, PactJsonObject, PactNull, PactJsonObject> {
		@Override
		public void reduce(Key key, Iterator<PactJsonObject> values, Collector<PactNull, PactJsonObject> out) {
		}
	}

	@Override
	public PactModule asPactModule() {
		if (this.getInputOperators().size() != 1)
			throw new UnsupportedOperationException();

		PactModule module = new PactModule(getInputOperators().size(), 1);
		for (int index = 0; index < getInputOperators().size(); index++) {
			// extract keys
		}

		// select strategy
		// MatchContract<Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> joinMatch = new
		// MatchContract<Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject>(
		// EquiJoinStub.class);
		// module.getOutput(0).setInput(joinMatch);
		// joinMatch.setInput(module.getInput(0));
		return module;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(this.getName());
		if (this.groupings != null)
			builder.append(" on ").append(this.groupings);
		if (this.getTransformation() != Transformation.IDENTITY)
			builder.append(" to ").append(this.getTransformation());
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 67;
		int result = super.hashCode();
		result = prime * result + groupings.hashCode();
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
		Aggregation other = (Aggregation) obj;
		if (!groupings.equals(other.groupings))
			return false;

		for (int index = 0; index < groupings.size(); index++)
			if (!groupings.get(index).equals(other.groupings.get(index)))
				return false;
		return true;
	}

}
