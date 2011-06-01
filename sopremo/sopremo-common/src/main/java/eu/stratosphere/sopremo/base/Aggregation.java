package eu.stratosphere.sopremo.base;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.Constant;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.Input;
import eu.stratosphere.sopremo.expressions.Path;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.util.dag.SubGraph;

public class Aggregation extends Operator {
	public final static List<EvaluableExpression> NO_GROUPING = new ArrayList<EvaluableExpression>();

	private List<? extends EvaluableExpression> groupings;

	public Aggregation(EvaluableExpression transformation, List<? extends EvaluableExpression> grouping,
			JsonStream... inputs) {
		super(transformation, inputs);
		if (grouping == null)
			throw new NullPointerException();
		this.groupings = grouping;
	}

	public Aggregation(EvaluableExpression transformation, List<? extends EvaluableExpression> grouping,
			List<? extends JsonStream> inputs) {
		super(transformation, inputs);
		if (grouping == null)
			throw new NullPointerException();
		this.groupings = grouping;
	}

	private void addSingleSourceAggregation(EvaluationContext context, PactModule module, List<Contract> keyExtractors) {
		ReduceContract<PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> aggregationReduce = new ReduceContract<PactJsonObject.Key, PactJsonObject, Key, PactJsonObject>(
			OneSourceAggregationStub.class);
		module.getOutput(0).setInput(aggregationReduce);
		aggregationReduce.setInput(keyExtractors.get(0));
		SopremoUtil.setTransformationAndContext(aggregationReduce.getStubParameters(), this.getTransformation(),
			context);
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		if (this.getInputOperators().size() > 2)
			throw new UnsupportedOperationException();

		PactModule module = new PactModule(this.getInputOperators().size(), 1);
		List<Contract> keyExtractors = new ArrayList<Contract>();
		for (EvaluableExpression grouping : this.groupings)
			keyExtractors.add(SopremoUtil.addKeyExtraction(module, grouping, context));

		switch (this.groupings.size()) {
		case 0:
			keyExtractors.add(SopremoUtil.addKeyExtraction(module, new Path(new Input(0), new Constant(1L)), context));
			this.addSingleSourceAggregation(context, module, keyExtractors);
			break;

		case 1:
			this.addSingleSourceAggregation(context, module, keyExtractors);
			break;

		default:
			CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> aggregationCoGroup = new CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject>(
				TwoSourceAggregationStub.class);
			module.getOutput(0).setInput(aggregationCoGroup);
			aggregationCoGroup.setFirstInput(keyExtractors.get(0));
			aggregationCoGroup.setSecondInput(keyExtractors.get(1));
			SopremoUtil.setTransformationAndContext(aggregationCoGroup.getStubParameters(), this.getTransformation(),
				context);
			break;
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
		Aggregation other = (Aggregation) obj;
		if (!this.groupings.equals(other.groupings))
			return false;

		for (int index = 0; index < this.groupings.size(); index++)
			if (!this.groupings.get(index).equals(other.groupings.get(index)))
				return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 67;
		int result = super.hashCode();
		result = prime * result + this.groupings.hashCode();
		return result;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(this.getName());
		if (this.groupings != null)
			builder.append(" on ").append(this.groupings);
		if (this.getTransformation() != EvaluableExpression.IDENTITY)
			builder.append(" to ").append(this.getTransformation());
		return builder.toString();
	}

	public static class OneSourceAggregationStub extends
			SopremoReduce<PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> {
		@Override
		public void reduce(PactJsonObject.Key key, final Iterator<PactJsonObject> values,
				Collector<Key, PactJsonObject> out) {
			JsonNode result = this.getTransformation().evaluate(JsonUtil.wrapWithNode(values), this.getContext());
			out.collect(PactNull.getInstance(), new PactJsonObject(result));
		}
	}

	public static class TwoSourceAggregationStub extends
			SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
		@Override
		public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<Key, PactJsonObject> out) {
			JsonNode result = this.getTransformation().evaluate(JsonUtil.wrapWithNode(values1, values2),
				this.getContext());
			out.collect(PactNull.getInstance(), new PactJsonObject(result));
		}
	}

}
