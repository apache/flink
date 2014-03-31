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

import eu.stratosphere.api.java.operators.TwoInputOperator;
import eu.stratosphere.api.java.operators.translation.BinaryNodeTranslation;
import eu.stratosphere.api.java.typeutils.TypeInformation;

public class DeltaIterativeDataSet<T, U> extends TwoInputOperator<T, U, T, DeltaIterativeDataSet<T, U>> {
	
	private DeltaIterativeDataSet<T, U> solutionSetPlaceholder;
	
	private int [] keyPositions ;
	
	private int maxIterations;

	DeltaIterativeDataSet(ExecutionEnvironment context, TypeInformation<T> type, DataSet<T> solutionSet, DataSet<U> workset, int [] keyPositions, int maxIterations) {
		super(solutionSet, workset, type);
		
		solutionSetPlaceholder = new DeltaIterativeDataSet<T, U>(context, type, solutionSet, workset, true);
		this.keyPositions = keyPositions;
		this.maxIterations = maxIterations;
	}
	
	private DeltaIterativeDataSet(ExecutionEnvironment context, TypeInformation<T> type, DataSet<T> solutionSet, DataSet<U> workset, boolean placeholder) {
		super(solutionSet, workset, type);
	}

	public DataSet<T> closeWith(DataSet<T> solutionsetResult, DataSet<U> worksetResult) {
		return new DeltaIterativeResultDataSet<T, U>(getExecutionEnvironment(), getType(), worksetResult.getType(), this, solutionSetPlaceholder, solutionsetResult, worksetResult, keyPositions, maxIterations);
	}
	
	public DataSet<T> getSolutionSet() {
		return solutionSetPlaceholder;
	}

	@Override
	protected BinaryNodeTranslation translateToDataFlow() {
		// All the translation magic happens when the iteration end is encountered.
		throw new UnsupportedOperationException("This should never happen.");
	}
}
