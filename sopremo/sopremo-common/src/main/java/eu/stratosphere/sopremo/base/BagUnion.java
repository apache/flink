package eu.stratosphere.sopremo.base;

import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoUtil;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;

public class BagUnion extends Operator {
	public BagUnion(Operator... inputs) {
		super(EvaluableExpression.IDENTITY, inputs);
	}

	public BagUnion(List<Operator> inputs) {
		super(EvaluableExpression.IDENTITY, inputs);
	}

	// TODO: replace with efficient union operator
	public static class TwoInputUnion extends
			SopremoCoGroup<PactNull, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> {
		@Override
		public void coGroup(PactNull key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<PactNull, PactJsonObject> out) {
			while (values1.hasNext())
				out.collect(key, values1.next());
			while (values2.hasNext())
				out.collect(key, values2.next());
		}
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		int numInputs = this.getInputOperators().size();
		PactModule module = new PactModule(numInputs, 1);

		Contract leftInput = module.getInput(0);
		for (int index = 1; index < numInputs; index++) {

			Contract rightInput = module.getInput(index);
			CoGroupContract<PactNull, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> union = new CoGroupContract<PactNull, PactJsonObject, PactJsonObject, PactNull, PactJsonObject>(
					TwoInputUnion.class);
			union.setFirstInput(leftInput);
			union.setSecondInput(rightInput);

			SopremoUtil.setTransformationAndContext(union.getStubParameters(), null, context);
			leftInput = union;
		}

		module.getOutput(0).setInput(leftInput);

		return module;
	}
}
