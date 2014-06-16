/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java;

import eu.stratosphere.api.common.accumulators.ConvergenceCriterion;
import eu.stratosphere.types.TypeInformation;

public class BulkIterationResultSet<T> extends DataSet<T> {

	private final IterativeDataSet<T> iterationHead;

	private final DataSet<T> nextPartialSolution;

	private final DataSet<?> terminationCriterion;
	
	private ConvergenceCriterion<?> convergenceCriterion;
	
	private String convergenceCriterionAccumulatorName;

	BulkIterationResultSet(ExecutionEnvironment context,
			TypeInformation<T> type,
			IterativeDataSet<T> iterationHead,
			DataSet<T> nextPartialSolution) {
		
		this(context, type, iterationHead, nextPartialSolution, null, null, null);
	}

	BulkIterationResultSet(ExecutionEnvironment context,
			TypeInformation<T> type, IterativeDataSet<T> iterationHead,
			DataSet<T> nextPartialSolution, DataSet<?> terminationCriterion) {
		
		this(context, type, iterationHead, nextPartialSolution, terminationCriterion, null, null);
	}
	
	BulkIterationResultSet(ExecutionEnvironment context,
			TypeInformation<T> type, IterativeDataSet<T> iterationHead,
			DataSet<T> nextPartialSolution, ConvergenceCriterion<?> convergenceCriterion,
			String convergenceCriterionAccumulatorName) {
		
		this(context, type, iterationHead, nextPartialSolution, null, convergenceCriterion, convergenceCriterionAccumulatorName);
	}

	BulkIterationResultSet(ExecutionEnvironment context,
			TypeInformation<T> type, IterativeDataSet<T> iterationHead,
			DataSet<T> nextPartialSolution, DataSet<?> terminationCriterion,
			ConvergenceCriterion<?> convergenceCriterion, String convergenceCriterionAccumulatorName) {
		
		super(context, type);
		this.iterationHead = iterationHead;
		this.nextPartialSolution = nextPartialSolution;
		this.terminationCriterion = terminationCriterion;
		this.convergenceCriterion = convergenceCriterion;
		this.convergenceCriterionAccumulatorName = convergenceCriterionAccumulatorName;
	}
	
	public IterativeDataSet<T> getIterationHead() {
		return iterationHead;
	}

	public DataSet<T> getNextPartialSolution() {
		return nextPartialSolution;
	}

	public DataSet<?> getTerminationCriterion() {
		return terminationCriterion;
	}

	public ConvergenceCriterion<?> getConvergenceCriterion() {
		return convergenceCriterion;
	}

	public String getConvergenceCriterionAccumulatorName() {
		return convergenceCriterionAccumulatorName;
	}
}
