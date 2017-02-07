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

import java.util.List;

import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.core.Window.RexWinAggCall;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.flink.api.java.tuple.Tuple5;

public class WindowAggregateUtil {


	public List<Tuple5<String,String,String,Integer,Integer>> getAggregateFunctions(LogicalWindow window){
		
		for(RexWinAggCall agg : window.groups.iterator().next().aggCalls){
			
		}
		return null;
		
	}

	/**
	 * A utility function that checks whether a window is partitioned
	 * or it is a global window.
	 * @param LogicalWindow window to be checked for partitions
	 * @return true if partition keys are defined, false otherwise.
	 */
	public boolean isStreamPartitioned(LogicalWindow window) {
		// if it exists a group bounded by keys, the it is
		// a partitioned window
		for(Group group:window.groups){
			if(!group.keys.isEmpty()){
				return true;
			}
		}
		
		return false;
	}

	public int[] getKeysAsArray(Group group) {
		if(group==null) { return null; }
		return group.keys.toArray();
	}
	
	
	
}
