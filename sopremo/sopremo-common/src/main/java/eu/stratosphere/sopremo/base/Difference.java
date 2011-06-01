package eu.stratosphere.sopremo.base;

import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;

public class Difference extends SetOperator {

	public Difference(List<Operator> inputs) {
		super(inputs);
	}

	public Difference(Operator... inputs) {
		super(inputs);
	}

	@Override
	protected Contract createSetContractForInputs(Contract leftInput, Contract rightInput) {
		CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> difference =
			new CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject>(
				TwoInputDifference.class);
		difference.setFirstInput(leftInput);
		difference.setSecondInput(rightInput);
		return difference;
	}

	public static class TwoInputDifference extends
			SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
		@Override
		public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<PactJsonObject.Key, PactJsonObject> out) {
			if (values1.hasNext() && !values2.hasNext())
				out.collect(key, values1.next());
		}
	}
}
