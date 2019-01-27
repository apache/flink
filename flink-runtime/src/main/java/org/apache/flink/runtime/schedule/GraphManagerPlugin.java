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

package org.apache.flink.runtime.schedule;

import org.apache.flink.runtime.event.ExecutionVertexFailoverEvent;
import org.apache.flink.runtime.event.ExecutionVertexStateChangedEvent;
import org.apache.flink.runtime.event.ResultPartitionConsumableEvent;
import org.apache.flink.runtime.jobgraph.JobGraph;

/**
 * Basic interface for graph manager plugin which handles execution events and decides which execution vertices to schedule.
 */
public interface GraphManagerPlugin {

	/**
	 * This method is called right after it is created, before the {@link VertexScheduler} is started.
	 */
	void open(VertexScheduler scheduler, JobGraph jobGraph, SchedulingConfig config);

	/**
	 * This method is called when the {@link VertexScheduler} is stopped.
	 */
	void close();

	/**
	 * Reset to initial state. It is invoked on graph manager reset.
	 */
	void reset();

	/**
	 * Notified when the scheduling is (re-)started.
	 */
	void onSchedulingStarted();

	/**
	 * Notified when a result partition is consumable.
	 */
	void onResultPartitionConsumable(ResultPartitionConsumableEvent event);

	/**
	 * Notified when any vertex state has changed.
	 */
	void onExecutionVertexStateChanged(ExecutionVertexStateChangedEvent event);

	/**
	 * Notified when vertex failover has happened and related vertices has been cancelled and reset.
	 * The graph manager plugin needs to decide how to re-schedule the affected vertices.
	 */
	void onExecutionVertexFailover(ExecutionVertexFailoverEvent event);

	/**
	 * Indicates whether execution vertices can be scheduled before knowing all of its upstream vertex locations.
	 */
	default boolean allowLazyDeployment() {
		return true;
	}
}
