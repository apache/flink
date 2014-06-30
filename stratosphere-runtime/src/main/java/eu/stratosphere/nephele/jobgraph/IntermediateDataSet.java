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

package eu.stratosphere.nephele.jobgraph;

/**
 * An intermediate data set is the data set produced by an operator - either a
 * source or any intermediate operation.
 * 
 * Intermediate data sets may be read by other operators, materialized, or
 * discarded.
 */
public class IntermediateDataSet {
	
	private final IntermediateDataSetID id; 		// the identifier
	
	private final AbstractJobVertex producer;		// the operation that produced this data set

	
	public IntermediateDataSet(AbstractJobVertex producer) {
		this(new IntermediateDataSetID(), producer);
	}
	
	public IntermediateDataSet(IntermediateDataSetID id, AbstractJobVertex producer) {
		this.id = id;
		this.producer = producer;
	}
	
	
}
