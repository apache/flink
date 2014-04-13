/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.compiler.contextcheck;

import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.BulkIteration;
import eu.stratosphere.api.common.operators.DualInputOperator;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.SingleInputOperator;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.util.Visitor;

/**
 * Traverses a plan and checks whether all Contracts are correctly connected to
 * their input contracts.
 */
public class ContextChecker implements Visitor<Operator> {

	/**
	 * A set of all already visited nodes during DAG traversal. Is used
	 * to avoid processing one node multiple times.
	 */
	private final Set<Operator> visitedNodes = new HashSet<Operator>();

	/**
	 * Default constructor
	 */
	public ContextChecker() {}

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
		this.visitedNodes.clear();
		plan.accept(this);
	}

	/**
	 * Checks whether the node is correctly connected to its input.
	 */
	@Override
	public boolean preVisit(Operator node) {
		// check if node was already visited
		if (!this.visitedNodes.add(node)) {
			return false;
		}

		// apply the appropriate check method
		if (node instanceof FileDataSink) {
			checkFileDataSink((FileDataSink) node);
		} else if (node instanceof FileDataSource) {
			checkFileDataSource((FileDataSource) node);
		} else if (node instanceof GenericDataSink) {
			checkDataSink((GenericDataSink) node);
		} else if (node instanceof BulkIteration) {
			checkBulkIteration((BulkIteration) node);
		} else if (node instanceof SingleInputOperator) {
			checkSingleInputContract((SingleInputOperator<?>) node);
		} else if (node instanceof DualInputOperator<?>) {
			checkDualInputContract((DualInputOperator<?>) node);
		}
		if(node instanceof Validatable) {
			((Validatable) node).check();
		}
		return true;
	}

	@Override
	public void postVisit(Operator node) {}

	/**
	 * Checks if DataSinkContract is correctly connected. In case that the
	 * contract is incorrectly connected a RuntimeException is thrown.
	 * 
	 * @param dataSinkContract
	 *        DataSinkContract that is checked.
	 */
	private void checkDataSink(GenericDataSink dataSinkContract) {
		Operator input = dataSinkContract.getInput();
		// check if input exists
		if (input == null) {
			throw new MissingChildException();
		}
	}
	
	/**
	 * Checks if FileDataSink is correctly connected. In case that the
	 * contract is incorrectly connected a RuntimeException is thrown.
	 * 
	 * @param fileSink
	 *        FileDataSink that is checked.
	 */
	private void checkFileDataSink(FileDataSink fileSink) {
		String path = fileSink.getFilePath();
		if (path == null) {
			throw new InvalidProgramException("File path of FileDataSink is null.");
		}
		if (path.length() == 0) {
			throw new InvalidProgramException("File path of FileDataSink is empty string.");
		}
		
		try {
			Path p = new Path(path);
			String scheme = p.toUri().getScheme();
			
			if (scheme == null) {
				throw new InvalidProgramException("File path \"" + path + "\" of FileDataSink has no file system scheme (like 'file:// or hdfs://').");
			}
		} catch (Exception e) {
			throw new InvalidProgramException("File path \"" + path + "\" of FileDataSink is an invalid path: " + e.getMessage());
		}
		checkDataSink(fileSink);
	}
	
	/**
	 * Checks if FileDataSource is correctly connected. In case that the
	 * contract is incorrectly connected a RuntimeException is thrown.
	 * 
	 * @param fileSource
	 *        FileDataSource that is checked.
	 */
	private void checkFileDataSource(FileDataSource fileSource) {
		String path = fileSource.getFilePath();
		if (path == null) {
			throw new InvalidProgramException("File path of FileDataSource is null.");
		}
		if (path.length() == 0) {
			throw new InvalidProgramException("File path of FileDataSource is empty string.");
		}
		
		try {
			Path p = new Path(path);
			String scheme = p.toUri().getScheme();
			
			if (scheme == null) {
				throw new InvalidProgramException("File path \"" + path + "\" of FileDataSource has no file system scheme (like 'file:// or hdfs://').");
			}
		} catch (Exception e) {
			throw new InvalidProgramException("File path \"" + path + "\" of FileDataSource is an invalid path: " + e.getMessage());
		}
	}

	/**
	 * Checks whether a SingleInputOperator is correctly connected. In case that
	 * the contract is incorrectly connected a RuntimeException is thrown.
	 * 
	 * @param singleInputContract
	 *        SingleInputOperator that is checked.
	 */
	private void checkSingleInputContract(SingleInputOperator<?> singleInputContract) {
		Operator input = singleInputContract.getInput();
		// check if input exists
		if (input == null) {
			throw new MissingChildException();
		}
	}

	/**
	 * Checks whether a DualInputOperator is correctly connected. In case that
	 * the contract is incorrectly connected a RuntimeException is thrown.
	 * 
	 * @param dualInputContract
	 *        DualInputOperator that is checked.
	 */
	private void checkDualInputContract(DualInputOperator<?> dualInputContract) {
		Operator input1 = dualInputContract.getFirstInput();
		Operator input2 = dualInputContract.getSecondInput();
		// check if input exists
		if (input1 == null || input2 == null) {
			throw new MissingChildException();
		}
	}
	
	private void checkBulkIteration(BulkIteration iter) {
		iter.validate();
		checkSingleInputContract(iter);
	}
}
