package eu.stratosphere.sopremo.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.AbstractIterator;
import eu.stratosphere.sopremo.expressions.Constant;
import eu.stratosphere.sopremo.expressions.Input;
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

	public static class OneSourceAggregationStub extends
			ReduceStub<PactJsonObject.Key, PactJsonObject, PactNull, PactJsonObject> {

		private Transformation transformation;

		@Override
		public void configure(Configuration parameters) {
			this.transformation = getTransformation(parameters, "transformation");
		}

		@Override
		public void reduce(PactJsonObject.Key key, final Iterator<PactJsonObject> values,
				Collector<PactNull, PactJsonObject> out) {
			final Iterator<JsonNode> transformedJsons = transformation.evaluate(new UnwrappingIterator(values));

			while (transformedJsons.hasNext())
				out.collect(PactNull.getInstance(), new PactJsonObject(transformedJsons.next()));
		}
	}

	public static class TwoSourceAggregationStub extends
			CoGroupStub<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> {
		private Transformation transformation;

		@Override
		public void configure(Configuration parameters) {
			this.transformation = getTransformation(parameters, "transformation");
		}

		@Override
		public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<PactNull, PactJsonObject> out) {
			final Iterator<JsonNode> transformedJsons = transformation.evaluate(new UnwrappingIterator(values1),
				new UnwrappingIterator(values2));

			while (transformedJsons.hasNext())
				out.collect(PactNull.getInstance(), new PactJsonObject(transformedJsons.next()));
		}
	}

	@Override
	public PactModule asPactModule() {
		if (this.getInputOperators().size() > 2)
			throw new UnsupportedOperationException();

		PactModule module = new PactModule(this.getInputOperators().size(), 1);
		List<Contract> keyExtractors = new ArrayList<Contract>();
		for (Path grouping : groupings)
			keyExtractors.add(addKeyExtraction(module, grouping));

		switch (groupings.size()) {
		case 0:
			keyExtractors.add(addKeyExtraction(module, new Path(new Input(0), new Constant(1L))));

		case 1:
			ReduceContract<PactJsonObject.Key, PactJsonObject, PactNull, PactJsonObject> aggregationReduce = new ReduceContract<PactJsonObject.Key, PactJsonObject, PactNull, PactJsonObject>(
				OneSourceAggregationStub.class);
			module.getOutput(0).setInput(aggregationReduce);
			aggregationReduce.setInput(keyExtractors.get(0));
			setTransformation(aggregationReduce.getStubParameters(), "transformation", this.getTransformation());
			break;

		default:
			CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> aggregationCoGroup = new CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject>(
					TwoSourceAggregationStub.class);
			module.getOutput(0).setInput(aggregationCoGroup);
			aggregationCoGroup.setFirstInput(keyExtractors.get(0));
			aggregationCoGroup.setSecondInput(keyExtractors.get(1));
			setTransformation(aggregationCoGroup.getStubParameters(), "transformation", this.getTransformation());
			break;
		}

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
		result = prime * result + this.groupings.hashCode();
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
		Aggregation other = (Aggregation) obj;
		if (!this.groupings.equals(other.groupings))
			return false;

		for (int index = 0; index < this.groupings.size(); index++)
			if (!this.groupings.get(index).equals(other.groupings.get(index)))
				return false;
		return true;
	}

}
