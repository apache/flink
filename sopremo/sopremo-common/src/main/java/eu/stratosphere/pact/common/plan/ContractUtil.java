package eu.stratosphere.pact.common.plan;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.SingleInputContract;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.stub.ReduceStub;

/**
 * Convenience methods when dealing with {@link Contract}s.
 * 
 * @author Arvid Heise
 */
public class ContractUtil {
	private final static Map<Class<?>, Class<?>> STUB_CONTRACTS = new HashMap<Class<?>, Class<?>>();

	static {
		STUB_CONTRACTS.put(MapStub.class, MapContract.class);
		STUB_CONTRACTS.put(ReduceStub.class, ReduceContract.class);
		STUB_CONTRACTS.put(CoGroupStub.class, CoGroupContract.class);
		STUB_CONTRACTS.put(CrossStub.class, CrossContract.class);
		STUB_CONTRACTS.put(MatchStub.class, MatchContract.class);
		STUB_CONTRACTS.put(FileInputFormat.class, FileDataSourceContract.class);
		STUB_CONTRACTS.put(FileOutputFormat.class, FileDataSinkContract.class);
	}

	/**
	 * Returns the associated {@link Contract} type for the given {@link Stub} class.
	 * 
	 * @param stubClass
	 *        the stub class
	 * @return the associated Contract type
	 */
	@SuppressWarnings({ "unchecked" })
	public static Class<? extends Contract> getContractClass(final Class<?> stubClass) {
		final Class<?> contract = STUB_CONTRACTS.get(stubClass);
		if (contract == null && stubClass != null)
			return getContractClass(stubClass.getSuperclass());
		return (Class<? extends Contract>) contract;
	}

	/**
	 * Returns a list of all inputs for the given {@link Contract}.<br>
	 * Currently, the list can have 0, 1, or 2 elements.
	 * 
	 * @param contract
	 *        the Contract whose inputs should be returned
	 * @return all input contracts to this contract
	 */
	public static Contract[] getInputs(final Contract contract) {
		if (contract instanceof FileDataSinkContract<?, ?>)
			return new Contract[] { ((FileDataSinkContract<?, ?>) contract).getInput() };
		if (contract instanceof SingleInputContract<?, ?, ?, ?>)
			return new Contract[] { ((SingleInputContract<?, ?, ?, ?>) contract).getInput() };
		if (contract instanceof DualInputContract<?, ?, ?, ?, ?, ?>)
			return new Contract[] { ((DualInputContract<?, ?, ?, ?, ?, ?>) contract).getFirstInput(),
				((DualInputContract<?, ?, ?, ?, ?, ?>) contract).getSecondInput() };
		return new Contract[0];
	}

	/**
	 * Sets the inputs of the given {@link Contract}.<br>
	 * Currently, the list can have 0, 1, or 2 elements and the number of elements must match the type of the contract.
	 * 
	 * @param contract
	 *        the Contract whose inputs should be set
	 * @param inputs
	 *        all input contracts to this contract
	 */
	public static void setInputs(final Contract contract, final Contract[] inputs) {
		if (contract instanceof FileDataSinkContract<?, ?>) {
			if (inputs.length != 1)
				throw new IllegalArgumentException("wrong number of inputs");
			((FileDataSinkContract<?, ?>) contract).setInput(inputs[0]);
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
