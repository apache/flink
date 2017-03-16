/*
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
package org.apache.flink.table.plan.logical.rel.util;

import java.io.Serializable;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.core.Window.RexWinAggCall;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.calcite.FlinkTypeFactory;

import com.google.common.collect.ImmutableList;

public class WindowAggregateUtil implements Serializable {

	private static final long serialVersionUID = -3916551736243544540L;

	private LogicalWindow windowPointer = null;

	public WindowAggregateUtil() {

	}

	public WindowAggregateUtil(LogicalWindow window) {
		this.windowPointer = window;

	}

	/**
	 * A utility function that checks whether a window is partitioned or it is a
	 * global window.
	 * 
	 * @param LogicalWindow
	 *            window to be checked for partitions
	 * @return true if partition keys are defined, false otherwise.
	 */
	public boolean isStreamPartitioned(LogicalWindow window) {
		// if it exists a group bounded by keys, the it is
		// a partitioned window
		for (Group group : window.groups) {
			if (!group.keys.isEmpty()) {
				return true;
			}
		}

		return false;
	}

	public int[] getKeysAsArray(Group group) {
		if (group == null) {
			return null;
		}
		return group.keys.toArray();
	}

	/**
	 * This method returns the [[int]] lowerbound of a window when expressed
	 * with an integer e.g. ... ROWS BETWEEN [[value]] PRECEDING AND CURRENT ROW
	 * 
	 * @param constants
	 *            the list of constant to get the offset value
	 * @return return the value of the lowerbound if available -1 otherwise
	 */

	
	public int getLowerBoundary(ImmutableList<RexLiteral> constants) {
		
		return ((Long)constants.get(1).getValue2()).intValue();

	}

	
	
	public long getLowerBoundary(RelNode input) { 
		RexInputRef ref = (RexInputRef) windowPointer.groups.asList().get(0).lowerBound.getOffset(); 
		int index = ref.getIndex(); 
		int count = input.getRowType().getFieldCount(); 
		int lowerboundIndex = index - count; 
		return ((java.math.BigDecimal) windowPointer.constants.get(lowerboundIndex).getValue2()).longValue(); 
		} 

	
	/**
	 * This method return the keys based on which the object is partitioned or null
	 * 
	 * @return return null for non-partitioned windows or the list of key indexes
	 */
	public int[] getPartitions() {
		Group windowBoundaries = windowPointer.groups.asList().get(0);
		int[] partitionKeys = null;

		if (windowBoundaries.keys.isEmpty()) {
			// partitionKeys = null is indicator of non partitioned OVER clause
		} else {
			partitionKeys = windowBoundaries.keys.toArray();
		}

		return partitionKeys;
	}

	/**
	 *  Get the list of aggregation functions to be applied within window. In principle it should be one group
	 * @param aggregators - return object of the aggregations 
	 * @param typeClasses - return object for the aggregations types
	 * @param indexes - return object for the indexes of the elements to apply the aggregations
	 */
	
	public void getAggregations(List<String> aggregators, List<TypeInformation<?>> typeOutput, List<Integer> indexes,  List<TypeInformation<?>> typeInput) {
		
		// for loop can be replaced with  windowPointer.groups.asList().get(0)
		for (final Group group : windowPointer.groups) {
			for (RexWinAggCall agg : group.aggCalls) {
				typeOutput.add(FlinkTypeFactory.toTypeInfo(agg.type));
				aggregators.add(agg.getKind().toString());
				indexes.add(((RexInputRef) agg.getOperands().get(0)).getIndex());
				typeInput.add(FlinkTypeFactory.toTypeInfo(((RexInputRef) agg.getOperands().get(0)).getType()));
			}
		}
		
	}

}
