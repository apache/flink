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

package eu.stratosphere.pact.testing;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.SingleInputContract;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * Adds missing {@link FileDataSourceContract} and {@link FileDataSinkContract} to an incomplete plan.
 * 
 * @author Arvid Heise
 */
class InputOutputAdder implements Visitor<Contract> {
	final Collection<Contract> contractsWithoutInput = new HashSet<Contract>();

	final Collection<Contract> contractsWithoutOutput = new HashSet<Contract>();

	private final LinkedList<Integer> inputs = new LinkedList<Integer>(Arrays.asList(0));

	private void addDefaultInput(final Contract contract) {
		if (contract instanceof FileDataSinkContract<?, ?>)
			((FileDataSinkContract<?, ?>) contract).addInput(TestPlan.createDefaultSource(contract.getName() + "-input"));
		else if (contract instanceof SingleInputContract<?, ?, ?, ?>)
			((SingleInputContract<?, ?, ?, ?>) contract).addInput(TestPlan.createDefaultSource(contract.getName()
				+ "-input"));
		else if (contract instanceof DualInputContract<?, ?, ?, ?, ?, ?>) {
			if (((DualInputContract<?, ?, ?, ?, ?, ?>) contract).getFirstInputs().size() == 0)
				((DualInputContract<?, ?, ?, ?, ?, ?>) contract).addFirstInput(TestPlan.createDefaultSource(contract
					.getName()
					+ "-input1"));
			if (((DualInputContract<?, ?, ?, ?, ?, ?>) contract).getSecondInputs().size() == 0)
				((DualInputContract<?, ?, ?, ?, ?, ?>) contract).addSecondInput(TestPlan.createDefaultSource(contract
					.getName()
					+ "-input2"));
		}
	}

	private Integer getExpectedInputs(final Class<?> clazz) {
		if (clazz == null)
			return 1;

		final Integer expected = EXPECTED_INPUTS.get(clazz);
		if (expected != null)
			return expected;

		return this.getExpectedInputs(clazz.getSuperclass());
	}

	@Override
	public void postVisit(final Contract visitable) {
		final Integer actualInputs = this.inputs.pop();
		if (this.inputs.size() == 1 && !(visitable instanceof FileDataSinkContract<?, ?>))
			this.contractsWithoutOutput.add(visitable);
		if (this.getExpectedInputs(visitable.getClass()) != actualInputs)
			this.contractsWithoutInput.add(visitable);
	}

	@Override
	public boolean preVisit(final Contract visitable) {
		this.inputs.push(this.inputs.pop() + 1);
		this.inputs.push(0);
		return true;
	}

	public Contract[] process(final Contract[] contracts) {
		final List<Contract> list = Arrays.asList(contracts);

		for (final Contract contract : contracts)
			contract.accept(this);

		for (final Contract contract : this.contractsWithoutOutput)
			list.set(list.indexOf(contract), this.replaceWithDefaultOutput(contract));

		for (final Contract contract : this.contractsWithoutInput)
			this.addDefaultInput(contract);

		return contracts;
	}

	private Contract replaceWithDefaultOutput(final Contract contract) {
		final FileDataSinkContract<Key, Value> defaultSink = TestPlan.createDefaultSink(contract.getName() + "-output");
		defaultSink.addInput(contract);
		return defaultSink;
	}

	@SuppressWarnings("serial")
	private final static Map<Class<? extends Contract>, Integer> EXPECTED_INPUTS = new HashMap<Class<? extends Contract>, Integer>() {
		{
			this.put(FileDataSourceContract.class, 0);
			this.put(FileDataSinkContract.class, 1);
			this.put(SingleInputContract.class, 1);
			this.put(DualInputContract.class, 2);
		}
	};
}