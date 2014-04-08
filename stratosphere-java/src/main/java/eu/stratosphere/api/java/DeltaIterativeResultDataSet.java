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

import eu.stratosphere.api.java.operators.Keys;
import eu.stratosphere.api.java.typeutils.TypeInformation;

public class DeltaIterativeResultDataSet<T, U> extends DataSet<T> {

	private DeltaIterativeDataSet<T, U> iterationHead;
	
	private DeltaIterativeDataSet<T, U> iterationHeadSolutionSet;

	private DataSet<T> nextSolutionSet;
	
	private DataSet<U> nextWorkset;
	
	private Keys<T> keys;
	
	private int maxIterations;
	
	private TypeInformation<U> typeWS;

	DeltaIterativeResultDataSet(ExecutionEnvironment context,
	                       TypeInformation<T> typeSS,
	                       TypeInformation<U> typeWS,
	                       DeltaIterativeDataSet<T, U> iterationHead,
	                       DeltaIterativeDataSet<T, U> iterationHeadSolutionSet,
	                       DataSet<T> nextSolutionSet,
	                       DataSet<U> nextWorkset,
	                       Keys<T> keys,
	                       int maxIterations)
	{
		super(context, typeSS);
		this.iterationHead = iterationHead;
		this.nextWorkset = nextWorkset;
		this.nextSolutionSet = nextSolutionSet;
		this.iterationHeadSolutionSet = iterationHeadSolutionSet;
		this.keys = keys;
		this.maxIterations = maxIterations;
		this.typeWS = typeWS;
	}

	public DeltaIterativeDataSet<T, U> getIterationHead() {
		return iterationHead;
	}
	
	public DeltaIterativeDataSet<T, U> getIterationHeadSolutionSet() {
		return iterationHeadSolutionSet;
	}

	public DataSet<T> getNextSolutionSet() {
		return nextSolutionSet;
	}

	public DataSet<U> getNextWorkset() {
		return nextWorkset;
	}
	
	public int [] getKeyPositions() {
		return keys.computeLogicalKeyPositions();
	}
	
	public int getMaxIterations() {
		return maxIterations;
	}
	
	public TypeInformation<U> getWorksetType() {
		return typeWS;
	}
}
