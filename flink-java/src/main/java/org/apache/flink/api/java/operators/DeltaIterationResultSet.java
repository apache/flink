/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.types.TypeInformation;

public class DeltaIterationResultSet<ST, WT> extends DataSet<ST> {

	private DeltaIteration<ST, WT> iterationHead;

	private DataSet<ST> nextSolutionSet;
	
	private DataSet<WT> nextWorkset;
	
	private Keys<ST> keys;
	
	private int maxIterations;
	
	private TypeInformation<WT> typeWS;

	DeltaIterationResultSet(ExecutionEnvironment context,
							TypeInformation<ST> typeSS,
							TypeInformation<WT> typeWS,
							DeltaIteration<ST, WT> iterationHead,
							DataSet<ST> nextSolutionSet,
							DataSet<WT> nextWorkset,
							Keys<ST> keys,
							int maxIterations)
	{
		super(context, typeSS);
		this.iterationHead = iterationHead;
		this.nextWorkset = nextWorkset;
		this.nextSolutionSet = nextSolutionSet;
		this.keys = keys;
		this.maxIterations = maxIterations;
		this.typeWS = typeWS;
	}

	public DeltaIteration<ST, WT> getIterationHead() {
		return iterationHead;
	}
	
	public DataSet<ST> getNextSolutionSet() {
		return nextSolutionSet;
	}

	public DataSet<WT> getNextWorkset() {
		return nextWorkset;
	}
	
	public int [] getKeyPositions() {
		return keys.computeLogicalKeyPositions();
	}
	
	public int getMaxIterations() {
		return maxIterations;
	}
	
	public TypeInformation<WT> getWorksetType() {
		return typeWS;
	}
}
