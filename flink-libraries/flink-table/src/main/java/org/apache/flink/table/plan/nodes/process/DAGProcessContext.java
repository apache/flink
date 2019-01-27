/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan.nodes.process;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.nodes.exec.BatchExecNode;
import org.apache.flink.table.resource.batch.NodeRunningUnit;

import java.util.Map;
import java.util.Set;

/**
 * Context for processors to process dag.
 */
public class DAGProcessContext {

	private final TableEnvironment tableEnvironment;

	private final Map<BatchExecNode<?>, Set<NodeRunningUnit>> runningUnitMap;

	public DAGProcessContext(TableEnvironment tableEnvironment, Map<BatchExecNode<?>, Set<NodeRunningUnit>> runningUnitMap) {
		this.tableEnvironment = tableEnvironment;
		this.runningUnitMap = runningUnitMap;
	}

	public DAGProcessContext(TableEnvironment tableEnvironment) {
		this(tableEnvironment, null);
	}

	/**
	 * Gets {@link TableEnvironment}, {@link org.apache.flink.table.api.BatchTableEnvironment} for batch job.
	 * and {@link org.apache.flink.table.api.StreamTableEnvironment} for stream job.
	 */
	public TableEnvironment getTableEnvironment() {
		return tableEnvironment;
	}

	/**
	 * Gets runningUnits map, runningUnits will be computed from all {@link BatchExecNode}s in advance. For batch job.
	 */
	public Map<BatchExecNode<?>, Set<NodeRunningUnit>> getRunningUnitMap() {
		return runningUnitMap;
	}
}
