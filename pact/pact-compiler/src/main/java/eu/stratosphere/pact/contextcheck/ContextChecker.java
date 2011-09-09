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

package eu.stratosphere.pact.contextcheck;

import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DualInputContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.SingleInputContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.Visitor;

/**
 * Traverses a plan and checks whether all Contracts are correctly connected to
 * their input contracts.
 * 
 * @author Max Heimel
 * @author Fabian Hueske
 */
public class ContextChecker implements Visitor<Contract> {

	public Set<Contract> visitedNodes = new HashSet<Contract>();

	/**
	 * Default constructor
	 */
	public ContextChecker() {
	}

	/**
	 * Checks whether the given plan is valid. In particular it is checked that
	 * all contracts have the correct number of inputs and all inputs are of the
	 * expected type. In case of an invalid plan an extended RuntimeException is
	 * thrown.
	 * 
	 * @param plan
	 *        The PACT plan to check.
	 */
	public void check(Plan plan) {
		visitedNodes.clear();
		plan.accept(this);
	}

	/**
	 * Checks whether the node is correctly connected to its input.
	 */
	@Override
	public boolean preVisit(Contract node) {

		// check if node was already visited
		if (visitedNodes.contains(node)) {
			return false;
		}

		// apply the appropriate check method
		if (node instanceof DataSinkContract<?, ?>) {
			checkDataSink((DataSinkContract<?, ?>) node);
		} else if (node instanceof MapContract<?, ?, ?, ?>) {
			checkSingleInputContract((MapContract<?, ?, ?, ?>) node);
		} else if (node instanceof ReduceContract<?, ?, ?, ?>) {
			checkSingleInputContract((ReduceContract<?, ?, ?, ?>) node);
		} else if (node instanceof MatchContract<?, ?, ?, ?, ?>) {
			checkDualInputContract((MatchContract<?, ?, ?, ?, ?>) node);
		} else if (node instanceof CoGroupContract<?, ?, ?, ?, ?>) {
			checkDualInputContract((CoGroupContract<?, ?, ?, ?, ?>) node);
		} else if (node instanceof CrossContract<?, ?, ?, ?, ?, ?>) {
			checkDualInputContract((CrossContract<?, ?, ?, ?, ?, ?>) node);
		}
		// Data sources must not be checked, since correctness of input type is
		// checked.

		// mark node as visited
		visitedNodes.add(node);

		return true;
	}

	@Override
	public void postVisit(Contract node) {
		// ignore post visits
	}

	/**
	 * Checks if DataSinkContract is correctly connected. In case that the
	 * contract is incorrectly connected a RuntimeException is thrown.
	 * 
	 * @param dataSinkContract
	 *        DataSinkContract that is checked.
	 */
	private void checkDataSink(DataSinkContract<?, ?> dataSinkContract) {

		Contract input = dataSinkContract.getInput();

		// check if input exists
		if (input == null) {
			throw new MissingChildException();
		}

		ContractInspector nodeInspector = new ContractInspector(dataSinkContract);
		ContractInspector inputInspector = new ContractInspector(input);

		Class<?> nodeInKeyClass = nodeInspector.getInput1KeyClass();
		Class<?> nodeInValueClass = nodeInspector.getInput1ValueClass();

		Class<?> inputOutKeyClass = inputInspector.getOutputKeyClass();
		Class<?> inputOutValueClass = inputInspector.getOutputValueClass();

		// check for correctness of input types
		if (!nodeInKeyClass.equals(inputOutKeyClass) || !nodeInValueClass.equals(inputOutValueClass)) {
			throw new ChannelTypeException();
		}

	}

	/**
	 * Checks whether a SingleInputContract is correctly connected. In case that
	 * the contract is incorrectly connected a RuntimeException is thrown.
	 * 
	 * @param singleInputContract
	 *        SingleInputContract that is checked.
	 */
	private void checkSingleInputContract(SingleInputContract<?, ?, ?, ?> singleInputContract) {

		Contract input = singleInputContract.getInput();

		// check if input exists
		if (input == null) {
			throw new MissingChildException();
		}

		ContractInspector nodeInspector = new ContractInspector(singleInputContract);
		ContractInspector inputInspector = new ContractInspector(input);

		Class<?> nodeInKeyClass = nodeInspector.getInput1KeyClass();
		Class<?> nodeInValueClass = nodeInspector.getInput1ValueClass();

		Class<?> inputOutKeyClass = inputInspector.getOutputKeyClass();
		Class<?> inputOutValueClass = inputInspector.getOutputValueClass();

		// check for correctness of input types
		if (!nodeInKeyClass.equals(inputOutKeyClass) || !nodeInValueClass.equals(inputOutValueClass)) {
			throw new ChannelTypeException();
		}
	}

	/**
	 * Checks whether a DualInputContract is correctly connected. In case that
	 * the contract is incorrectly connected a RuntimeException is thrown.
	 * 
	 * @param dualInputContract
	 *        DualInputContract that is checked.
	 */
	private void checkDualInputContract(DualInputContract<?, ?, ?, ?, ?, ?> dualInputContract) {
		Contract input1 = dualInputContract.getFirstInput();
		Contract input2 = dualInputContract.getSecondInput();

		// check if input exists
		if (input1 == null || input2 == null) {
			throw new MissingChildException();
		}

		ContractInspector nodeInspector = new ContractInspector(dualInputContract);
		ContractInspector input1Inspector = new ContractInspector(input1);
		ContractInspector input2Inspector = new ContractInspector(input2);

		Class<?> nodeInKey1Class = nodeInspector.getInput1KeyClass();
		Class<?> nodeInValue1Class = nodeInspector.getInput1ValueClass();

		Class<?> nodeInKey2Class = nodeInspector.getInput2KeyClass();
		Class<?> nodeInValue2Class = nodeInspector.getInput2ValueClass();

		Class<?> input1OutKeyClass = input1Inspector.getOutputKeyClass();
		Class<?> input1OutValueClass = input1Inspector.getOutputValueClass();

		Class<?> input2OutKeyClass = input2Inspector.getOutputKeyClass();
		Class<?> input2OutValueClass = input2Inspector.getOutputValueClass();

		// check for correctness of input types
		if (!nodeInKey1Class.equals(input1OutKeyClass) || !nodeInValue1Class.equals(input1OutValueClass)
			|| !nodeInKey2Class.equals(input2OutKeyClass) || !nodeInValue2Class.equals(input2OutValueClass)) {
			throw new ChannelTypeException();
		}
	}

}
