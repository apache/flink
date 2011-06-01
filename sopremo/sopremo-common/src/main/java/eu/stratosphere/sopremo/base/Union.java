package eu.stratosphere.sopremo.base;

import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;

public class Union extends SetOperator {
	public Union(List<Operator> inputs) {
		super(inputs);
	}

	public Union(Operator... inputs) {
		super(inputs);
	}

	@Override
	protected Contract createSetContractForInputs(Contract leftInput, Contract rightInput) {
		CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> union =
			new CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject>(
				TwoInputUnion.class);
		union.setFirstInput(leftInput);
		union.setSecondInput(rightInput);
		return union;
	}

	public static class TwoInputUnion extends
			SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
		@Override
		public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<PactJsonObject.Key, PactJsonObject> out) {
			if (values1.hasNext())
				out.collect(key, values1.next());
			else if (values2.hasNext())
				out.collect(key, values2.next());
		}
	}
}
