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

import java.util.List;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.plan.ContractUtil;
import eu.stratosphere.pact.common.plan.Visitor;

/**
 * Adds missing {@link DataSourceContract} and {@link DataSinkContract} to an incomplete plan.
 * 
 * @author Arvid Heise
 */
class InputOutputAdder implements Visitor<Contract> {
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.Visitor#preVisit(eu.stratosphere.pact.common.plan.Visitable)
	 */
	@Override
	public boolean preVisit(Contract visitable) {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.Visitor#postVisit(eu.stratosphere.pact.common.plan.Visitable)
	 */
	@Override
	public void postVisit(Contract contract) {
		List<List<Contract>> inputs = ContractUtil.getInputs(contract);
		for (int index = 0; index < inputs.size(); index++)
			if (inputs.get(index).isEmpty())
				inputs.get(index).add(
					TestPlan.createDefaultSource(String.format("%s-input%d", contract.getName(), index)));
		ContractUtil.setInputs(contract, inputs);
	}

	public Contract[] process(final Contract[] contracts) {
		for (Contract contract : contracts)
			contract.accept(this);

		for (int index = 0; index < contracts.length; index++)
			if (!(contracts[index] instanceof GenericDataSink))
				contracts[index] = this.replaceWithDefaultOutput(contracts[index]);

		return contracts;
	}

	private Contract replaceWithDefaultOutput(final Contract contract) {
		final FileDataSink defaultSink = TestPlan.createDefaultSink(contract.getName() + "-output");
		defaultSink.addInput(contract);
		return defaultSink;
	}
}