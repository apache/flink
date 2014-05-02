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

package eu.stratosphere.pact.runtime.task;

import static eu.stratosphere.pact.runtime.task.DamBehavior.FULL_DAM;
import static eu.stratosphere.pact.runtime.task.DamBehavior.MATERIALIZING;
import static eu.stratosphere.pact.runtime.task.DamBehavior.PIPELINED;
import eu.stratosphere.pact.runtime.task.chaining.ChainedCollectorMapDriver;
import eu.stratosphere.pact.runtime.task.chaining.ChainedDriver;
import eu.stratosphere.pact.runtime.task.chaining.ChainedFlatMapDriver;
import eu.stratosphere.pact.runtime.task.chaining.ChainedMapDriver;
import eu.stratosphere.pact.runtime.task.chaining.SynchronousChainedCombineDriver;

/**
 * Enumeration of all available operator strategies. 
 */
public enum DriverStrategy {
	// no local strategy, as for sources and sinks
	NONE(null, null, PIPELINED, false),
	// a unary no-op operator
	UNARY_NO_OP(NoOpDriver.class, null, PIPELINED, PIPELINED, false),
	// a binary no-op operator
	BINARY_NO_OP(null, null, PIPELINED, PIPELINED, false),
	// the old mapper
	COLLECTOR_MAP(CollectorMapDriver.class, ChainedCollectorMapDriver.class, PIPELINED, false),
	// the proper mapper
	MAP(MapDriver.class, ChainedMapDriver.class, PIPELINED, false),
	// the flat mapper
	FLAT_MAP(FlatMapDriver.class, ChainedFlatMapDriver.class, PIPELINED, false),
	// grouping the inputs
	SORTED_GROUP(ReduceDriver.class, null, PIPELINED, true),
	// partially grouping inputs (best effort resulting possibly in duplicates --> combiner)
	PARTIAL_GROUP(CombineDriver.class, SynchronousChainedCombineDriver.class, MATERIALIZING, true),
	// group everything together into one group
	ALL_GROUP(AllReduceDriver.class, null, PIPELINED, false),
	// already grouped input, within a key values are crossed in a nested loop fashion
	GROUP_SELF_NESTEDLOOP(null, null, PIPELINED, true),	// Note: Self-Match currently inactive
	// both inputs are merged, but materialized to the side for block-nested-loop-join among values with equal key
	MERGE(MatchDriver.class, null, MATERIALIZING, MATERIALIZING, true),
	// co-grouping inputs
	CO_GROUP(CoGroupDriver.class, null, PIPELINED, PIPELINED, true),
	// the first input is build side, the second side is probe side of a hybrid hash table
	HYBRIDHASH_BUILD_FIRST(MatchDriver.class, null, FULL_DAM, MATERIALIZING, true),
	// the second input is build side, the first side is probe side of a hybrid hash table
	HYBRIDHASH_BUILD_SECOND(MatchDriver.class, null, MATERIALIZING, FULL_DAM, true),
	// the second input is inner loop, the first input is outer loop and block-wise processed
	NESTEDLOOP_BLOCKED_OUTER_FIRST(CrossDriver.class, null, MATERIALIZING, MATERIALIZING, false),
	// the first input is inner loop, the second input is outer loop and block-wise processed
	NESTEDLOOP_BLOCKED_OUTER_SECOND(CrossDriver.class, null, MATERIALIZING, MATERIALIZING, false),
	// the second input is inner loop, the first input is outer loop and stream-processed
	NESTEDLOOP_STREAMED_OUTER_FIRST(CrossDriver.class, null, PIPELINED, MATERIALIZING, false),
	// the first input is inner loop, the second input is outer loop and stream-processed
	NESTEDLOOP_STREAMED_OUTER_SECOND(CrossDriver.class, null, MATERIALIZING, PIPELINED, false),
	// union utility op. unions happen implicitly on the network layer (in the readers) when bundeling streams
	UNION(null, null, FULL_DAM, FULL_DAM, false);
	// explicit binary union between a streamed and a cached input
//	UNION_WITH_CACHED(UnionWithTempOperator.class, null, FULL_DAM, PIPELINED, false);
	
	// --------------------------------------------------------------------------------------------
	
	private final Class<? extends PactDriver<?, ?>> driverClass;
	
	private final Class<? extends ChainedDriver<?, ?>> pushChainDriver;
	
	private final DamBehavior dam1;
	private final DamBehavior dam2;
	
	private final int numInputs;
	
	private final boolean requiresComparator;
	

	@SuppressWarnings("unchecked")
	private DriverStrategy(
			@SuppressWarnings("rawtypes") Class<? extends PactDriver> driverClass, 
			@SuppressWarnings("rawtypes") Class<? extends ChainedDriver> pushChainDriverClass, 
			DamBehavior dam, boolean comparator)
	{
		this.driverClass = (Class<? extends PactDriver<?, ?>>) driverClass;
		this.pushChainDriver = (Class<? extends ChainedDriver<?, ?>>) pushChainDriverClass;
		this.numInputs = 1;
		this.dam1 = dam;
		this.dam2 = null;
		this.requiresComparator = comparator;
	}
	
	@SuppressWarnings("unchecked")
	private DriverStrategy(
			@SuppressWarnings("rawtypes") Class<? extends PactDriver> driverClass, 
			@SuppressWarnings("rawtypes") Class<? extends ChainedDriver> pushChainDriverClass, 
			DamBehavior firstDam, DamBehavior secondDam, boolean comparator)
	{
		this.driverClass = (Class<? extends PactDriver<?, ?>>) driverClass;
		this.pushChainDriver = (Class<? extends ChainedDriver<?, ?>>) pushChainDriverClass;
		this.numInputs = 2;
		this.dam1 = firstDam;
		this.dam2 = secondDam;
		this.requiresComparator = comparator;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public Class<? extends PactDriver<?, ?>> getDriverClass() {
		return this.driverClass;
	}
	
	public Class<? extends ChainedDriver<?, ?>> getPushChainDriverClass() {
		return this.pushChainDriver;
	}
	
	public int getNumInputs() {
		return this.numInputs;
	}
	
	public DamBehavior firstDam() {
		return this.dam1;
	}
	
	public DamBehavior secondDam() {
		if (this.numInputs == 2) {
			return this.dam2;
		} else {
			throw new IllegalArgumentException("The given strategy does not work on two inputs.");
		}
	}
	
	public DamBehavior damOnInput(int num) {
		if (num < this.numInputs) {
			if (num == 0) {
				return this.dam1;
			} else if (num == 1) {
				return this.dam2;
			}
		}
		throw new IllegalArgumentException();
	}
	
	public boolean isMaterializing() {
		return this.dam1.isMaterializing() || (this.dam2 != null && this.dam2.isMaterializing());
	}
	
	public boolean requiresComparator() {
		return this.requiresComparator;
	}
}
