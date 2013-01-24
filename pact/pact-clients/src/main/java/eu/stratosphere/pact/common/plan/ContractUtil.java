/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.common.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.generic.contract.DualInputContract;
import eu.stratosphere.pact.generic.contract.SingleInputContract;
import eu.stratosphere.pact.generic.io.InputFormat;
import eu.stratosphere.pact.generic.io.OutputFormat;

/**
 * Convenience methods when dealing with {@link Contract}s.
 * 
 * @author Arvid Heise
 */
public class ContractUtil {
	private final static Map<Class<?>, Class<? extends Contract>> STUB_CONTRACTS =
		new LinkedHashMap<Class<?>, Class<? extends Contract>>();

	static {
		STUB_CONTRACTS.put(MapStub.class, MapContract.class);
		STUB_CONTRACTS.put(ReduceStub.class, ReduceContract.class);
		STUB_CONTRACTS.put(CoGroupStub.class, CoGroupContract.class);
		STUB_CONTRACTS.put(CrossStub.class, CrossContract.class);
		STUB_CONTRACTS.put(MatchStub.class, MatchContract.class);
		STUB_CONTRACTS.put(FileInputFormat.class, FileDataSource.class);
		STUB_CONTRACTS.put(FileOutputFormat.class, FileDataSink.class);
		STUB_CONTRACTS.put(InputFormat.class, GenericDataSource.class);
		STUB_CONTRACTS.put(OutputFormat.class, GenericDataSink.class);
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
		if (stubClass == null)
			return null;
		final Class<?> contract = STUB_CONTRACTS.get(stubClass);
		if (contract != null)
			return (Class<? extends Contract>) contract;
		Iterator<Entry<Class<?>, Class<? extends Contract>>> stubContracts = STUB_CONTRACTS.entrySet().iterator();
		while (stubContracts.hasNext()) {
			Map.Entry<Class<?>, Class<? extends Contract>> entry = stubContracts.next();
			if (entry.getKey().isAssignableFrom(stubClass))
				return entry.getValue();
		}
		return null;

	}

	/**
	 * Returns the number of inputs for the given {@link Contract} type.<br>
	 * Currently, it can be 0, 1, or 2.
	 * 
	 * @param contractClass
	 *        the type of the Contract
	 * @return the number of input contracts
	 */
	public static int getNumInputs(final Class<? extends Contract> contractType) {

		if (GenericDataSource.class.isAssignableFrom(contractType))
			return 0;
		if (GenericDataSink.class.isAssignableFrom(contractType)
			|| SingleInputContract.class.isAssignableFrom(contractType))
			return 1;
		if (DualInputContract.class.isAssignableFrom(contractType))
			return 2;
		throw new IllegalArgumentException("not supported");
	}

	/**
	 * Returns a list of all inputs for the given {@link Contract}.<br>
	 * Currently, the list can have 0, 1, or 2 elements.
	 * 
	 * @param contract
	 *        the Contract whose inputs should be returned
	 * @return all input contracts to this contract
	 */
	public static List<List<Contract>> getInputs(final Contract contract) {
		ArrayList<List<Contract>> inputs = new ArrayList<List<Contract>>();

		if (contract instanceof GenericDataSink)
			inputs.add(new ArrayList<Contract>(((GenericDataSink) contract).getInputs()));
		else if (contract instanceof SingleInputContract)
			inputs.add(new ArrayList<Contract>(((SingleInputContract<?>) contract).getInputs()));
		else if (contract instanceof DualInputContract) {
			inputs.add(new ArrayList<Contract>(((DualInputContract<?>) contract).getFirstInputs()));
			inputs.add(new ArrayList<Contract>(((DualInputContract<?>) contract).getSecondInputs()));
		}
		return inputs;
	}

	public static List<Contract> getFlatInputs(final Contract contract) {
		if (contract instanceof GenericDataSink)
			return ((GenericDataSink) contract).getInputs();
		if (contract instanceof SingleInputContract)
			return ((SingleInputContract<?>) contract).getInputs();
		if (contract instanceof DualInputContract) {
			ArrayList<Contract> inputs = new ArrayList<Contract>();
			inputs.addAll(((DualInputContract<?>) contract).getFirstInputs());
			inputs.addAll(((DualInputContract<?>) contract).getSecondInputs());
			return inputs;
		}

		return new ArrayList<Contract>();
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
	public static void setInputs(final Contract contract, final List<List<Contract>> inputs) {
		if (contract instanceof GenericDataSink) {
			if (inputs.size() != 1)
				throw new IllegalArgumentException("wrong number of inputs");
			((GenericDataSink) contract).setInputs(inputs.get(0));
		} else if (contract instanceof SingleInputContract) {
			if (inputs.size() != 1)
				throw new IllegalArgumentException("wrong number of inputs");
			((SingleInputContract<?>) contract).setInputs(inputs.get(0));
		} else if (contract instanceof DualInputContract) {
			if (inputs.size() != 2)
				throw new IllegalArgumentException("wrong number of inputs");
			((DualInputContract<?>) contract).setFirstInputs(inputs.get(0));
			((DualInputContract<?>) contract).setSecondInputs(inputs.get(1));
		}
	}

	/**
	 * Swaps two inputs of the given contract.
	 * 
	 * @param contract
	 *        the contract
	 * @param input1
	 *        the first input index
	 * @param input2
	 *        the second input index
	 */
	public static void swapInputs(Contract contract, int input1, int input2) {
		final List<List<Contract>> inputs = new ArrayList<List<Contract>>(getInputs(contract));
		Collections.swap(inputs, input1, input2);
		setInputs(contract, inputs);
	}
}
