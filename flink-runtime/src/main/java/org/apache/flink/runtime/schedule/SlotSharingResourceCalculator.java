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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;

/**
 * Calculates the resources of the whole SlotSharingGroup. If the slot sharing
 * is enabled, the vertices inside the sharing group should use the resource of
 * the sharing group when requesting slots.
 */
public interface SlotSharingResourceCalculator {

	/**
	 * Calculate the resource of the whole sharing group.
	 *
	 * @param slotSharingGroup the sharing group to calculate resource.
	 * @param executionGraph the execution graph that contains the sharing group.
	 * @return the resource profile of the target sharing group.
	 */
	ResourceProfile calculateSharedGroupResource(SlotSharingGroup slotSharingGroup, ExecutionGraph executionGraph);

}
