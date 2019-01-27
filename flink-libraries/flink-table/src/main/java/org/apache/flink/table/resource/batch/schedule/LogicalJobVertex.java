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

package org.apache.flink.table.resource.batch.schedule;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Logical jobVertex.
 */
public class LogicalJobVertex {

	private final JobVertex jobVertex;
	private final AtomicInteger deployedNum = new AtomicInteger();
	private Set<LogicalJobVertexRunningUnit> runningUnitSet = new LinkedHashSet<>();

	public LogicalJobVertex(JobVertex jobVertex) {
		this.jobVertex = jobVertex;
	}

	public void deployingTask() {
		deployedNum.incrementAndGet();
	}

	public boolean allTasksDeploying() {
		return deployedNum.get() >= jobVertex.getParallelism();
	}

	public int getParallelism() {
		return jobVertex.getParallelism();
	}

	public JobVertexID getJobVertexID() {
		return jobVertex.getID();
	}

	public void addRunningUnit(LogicalJobVertexRunningUnit runningUnit) {
		this.runningUnitSet.add(runningUnit);
	}

	public Set<LogicalJobVertexRunningUnit> getRunningUnitSet() {
		return runningUnitSet;
	}

	public void failoverTask() {
		deployedNum.decrementAndGet();
	}

	@Override
	public String toString() {
		return jobVertex.getName();
	}
}
