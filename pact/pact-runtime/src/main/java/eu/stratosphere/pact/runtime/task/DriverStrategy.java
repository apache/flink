/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.task;

/**
 * Enumeration of all available local processing strategies tasks. 
 */
public enum DriverStrategy
{
	// no special local strategy is applied
	NONE(MapDriver.class, false, false),
	// grouping the inputs
	GROUP(ReduceDriver.class, false, true),
	// already grouped input, within a key values are crossed in a nested loop fashion
	GROUP_SELF_NESTEDLOOP(null, false, true),	// Note: Self-Match currently inactive
	// both inputs are merged
	MERGE(MatchDriver.class, false, false, true),
	// co-grouping inputs
	CO_GROUP(CoGroupDriver.class, false, false, true),
	// the first input is build side, the second side is probe side of a hybrid hash table
	HYBRIDHASH_FIRST(MatchDriver.class, true, false, true),
	// the second input is build side, the first side is probe side of a hybrid hash table
	HYBRIDHASH_SECOND(MatchDriver.class, false, true, true),
	// the second input is inner loop, the first input is outer loop and block-wise processed
	NESTEDLOOP_BLOCKED_OUTER_FIRST(CrossDriver.class, false, true, false),
	// the first input is inner loop, the second input is outer loop and block-wise processed
	NESTEDLOOP_BLOCKED_OUTER_SECOND(CrossDriver.class, true, false, false),
	// the second input is inner loop, the first input is outer loop and stream-processed
	NESTEDLOOP_STREAMED_OUTER_FIRST(CrossDriver.class, false, true, false),
	// the first input is inner loop, the second input is outer loop and stream-processed
	NESTEDLOOP_STREAMED_OUTER_SECOND(CrossDriver.class, true, false, false);
	
	// --------------------------------------------------------------------------------------------
	
	private final Class<? extends PactDriver<?, ?>> driverClass;
	
	private final int numInputs;
	
	private final boolean dam1;
	private final boolean dam2;
	
	private final boolean requiresComparator;

	@SuppressWarnings("unchecked")
	private DriverStrategy(@SuppressWarnings("rawtypes") Class<? extends PactDriver> driverClass, boolean dams, boolean comparator) {
		this.driverClass = (Class<? extends PactDriver<?, ?>>) driverClass;
		this.numInputs = 1;
		this.dam1 = dams;
		this.dam2 = false;
		this.requiresComparator = comparator;
	}
	
	@SuppressWarnings("unchecked")
	private DriverStrategy(@SuppressWarnings("rawtypes") Class<? extends PactDriver> driverClass, boolean damsFirst, boolean damsSecond, boolean comparator) {
		this.driverClass = (Class<? extends PactDriver<?, ?>>) driverClass;
		this.numInputs = 2;
		this.dam1 = damsFirst;
		this.dam2 = damsSecond;
		this.requiresComparator = comparator;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public Class<? extends PactDriver<?, ?>> getDriverClass() {
		return this.driverClass;
	}
	
	public int getNumInputs() {
		return this.numInputs;
	}
	
	public boolean damsFirst() {
		return this.dam1;
	}
	
	public boolean damsSecond() {
		if (this.numInputs == 2) {
			return this.dam2;
		} else {
			throw new IllegalArgumentException("The given strategy does not work on two inputs.");
		}
	}
	
	public boolean damsInput(int num) {
		if (num < this.numInputs) {
			if (num == 0) {
				return this.dam1;
			} else if (num == 1) {
				return this.dam2;
			}
		}
		throw new IllegalArgumentException();
	}
	
	public int getNumberOfDams() {
		return (this.dam1 ? 1 : 0) + (this.dam2 ? 1 : 0);
	}
	
	public boolean requiresComparator() {
		return this.requiresComparator;
	}
}