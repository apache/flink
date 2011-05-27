package eu.stratosphere.sopremo.operator;

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
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.DataStream;
import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtils;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.Constant;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.Input;
import eu.stratosphere.sopremo.expressions.Path;

public class Aggregation extends Operator {
	public final static List<EvaluableExpression> NO_GROUPING = new ArrayList<EvaluableExpression>();

	private List<? extends EvaluableExpression> groupings;

	public Aggregation(Evaluable transformation, List<? extends EvaluableExpression> grouping, DataStream... inputs) {
		super(transformation, inputs);
		if (grouping == null)
			throw new NullPointerException();
		this.groupings = grouping;
	}

	public Aggregation(Evaluable transformation, List<? extends EvaluableExpression> grouping,
			List<? extends DataStream> inputs) {
		super(transformation, inputs);
		if (grouping == null)
			throw new NullPointerException();
		this.groupings = grouping;
	}

	public static class OneSourceAggregationStub extends
			SopremoReduce<PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> {
		@Override
		public void reduce(PactJsonObject.Key key, final Iterator<PactJsonObject> values,
				Collector<Key, PactJsonObject> out) {
			JsonNode result = this.getTransformation().evaluate(new StreamArrayNode(new UnwrappingIterator(values)),
				this.getContext());
			out.collect(PactNull.getInstance(), new PactJsonObject(result));
		}
	}

	public static class TwoSourceAggregationStub extends
			SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
		@Override
		public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<Key, PactJsonObject> out) {
			JsonNode result = this.getTransformation().evaluate(JsonUtils.asArray(
				new StreamArrayNode(new UnwrappingIterator(values1)),
				new StreamArrayNode(new UnwrappingIterator(values2))), this.getContext());
			out.collect(PactNull.getInstance(), new PactJsonObject(result));
		}
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		if (this.getInputOperators().size() > 2)
			throw new UnsupportedOperationException();

		PactModule module = new PactModule(this.getInputOperators().size(), 1);
		List<Contract> keyExtractors = new ArrayList<Contract>();
		for (EvaluableExpression grouping : this.groupings)
			keyExtractors.add(PactUtil.addKeyExtraction(module, grouping, context));

		switch (this.groupings.size()) {
		case 0:
			keyExtractors.add(PactUtil.addKeyExtraction(module, new Path(new Input(0), new Constant(1L)), context));
			addSingleSourceAggregation(context, module, keyExtractors);
			break;

		case 1:
			addSingleSourceAggregation(context, module, keyExtractors);
			break;

		default:
			CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> aggregationCoGroup = new CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, Key, PactJsonObject>(
				TwoSourceAggregationStub.class);
			module.getOutput(0).setInput(aggregationCoGroup);
			aggregationCoGroup.setFirstInput(keyExtractors.get(0));
			aggregationCoGroup.setSecondInput(keyExtractors.get(1));
			PactUtil.setTransformationAndContext(aggregationCoGroup.getStubParameters(), this.getEvaluableExpression(),
				context);
			break;
		}

		return module;
	}

	private void addSingleSourceAggregation(EvaluationContext context, PactModule module, List<Contract> keyExtractors) {
		ReduceContract<PactJsonObject.Key, PactJsonObject, Key, PactJsonObject> aggregationReduce = new ReduceContract<PactJsonObject.Key, PactJsonObject, Key, PactJsonObject>(
			OneSourceAggregationStub.class);
		module.getOutput(0).setInput(aggregationReduce);
		aggregationReduce.setInput(keyExtractors.get(0));
		PactUtil.setTransformationAndContext(aggregationReduce.getStubParameters(), this.getEvaluableExpression(),
			context);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(this.getName());
		if (this.groupings != null)
			builder.append(" on ").append(this.groupings);
		if (this.getEvaluableExpression() != EvaluableExpression.IDENTITY)
			builder.append(" to ").append(this.getEvaluableExpression());
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
