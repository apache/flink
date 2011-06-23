package eu.stratosphere.pact.common.plan;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.contract.SingleInputContract;

public class ContractUtil {
	public static Contract[] getInputs(Contract contract) {
		if (contract instanceof DataSinkContract<?, ?>)
			return new Contract[] { ((DataSinkContract<?, ?>) contract).getInput() };
		if (contract instanceof SingleInputContract<?, ?, ?, ?>)
			return new Contract[] { ((SingleInputContract<?, ?, ?, ?>) contract).getInput() };
		if (contract instanceof DualInputContract<?, ?, ?, ?, ?, ?>)
			return new Contract[] { ((DualInputContract<?, ?, ?, ?, ?, ?>) contract).getFirstInput(),
				((DualInputContract<?, ?, ?, ?, ?, ?>) contract).getSecondInput() };
		return new Contract[0];
	}

	public static void setInputs(Contract contract, Contract[] inputs) {
		if (contract instanceof DataSinkContract<?, ?>) {
			if (inputs.length != 1)
				throw new IllegalArgumentException("wrong number of inputs");
			((DataSinkContract<?, ?>) contract).setInput(inputs[0]);
		} else if (contract instanceof SingleInputContract<?, ?, ?, ?>) {
			if (inputs.length != 1)
				throw new IllegalArgumentException("wrong number of inputs");
			((SingleInputContract<?, ?, ?, ?>) contract).setInput(inputs[0]);
		} else if (contract instanceof DualInputContract<?, ?, ?, ?, ?, ?>) {
			if (inputs.length != 2)
				throw new IllegalArgumentException("wrong number of inputs");
			((DualInputContract<?, ?, ?, ?, ?, ?>) contract).setFirstInput(inputs[0]);
			((DualInputContract<?, ?, ?, ?, ?, ?>) contract).setSecondInput(inputs[1]);
		}
	}
}
