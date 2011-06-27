package eu.stratosphere.sopremo.base;

import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class UnionAll extends ElementaryOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8160253166221264064L;

	public UnionAll(List<Operator> inputs) {
		super(inputs);
	}

	public UnionAll(Operator... inputs) {
		super(inputs);
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		int numInputs = this.getInputOperators().size();
		PactModule module = new PactModule(this.toString(), numInputs, 1);

		Contract leftInput = module.getInput(0);
		for (int index = 1; index < numInputs; index++) {

			Contract rightInput = module.getInput(index);
			CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> union =
				new CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject>(
					TwoInputUnion.class);
			union.setFirstInput(leftInput);
			union.setSecondInput(rightInput);

			SopremoUtil.setContext(union.getStubParameters(), context);
			leftInput = union;
		}

		module.getOutput(0).setInput(leftInput);

		return module;
	}

	// TODO: replace with efficient union operator
	public static class TwoInputUnion extends
			SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
		@Override
		public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<PactJsonObject.Key, PactJsonObject> out) {
			while (values1.hasNext())
				out.collect(key, values1.next());
			while (values2.hasNext())
				out.collect(key, values2.next());
		}
	}
}
