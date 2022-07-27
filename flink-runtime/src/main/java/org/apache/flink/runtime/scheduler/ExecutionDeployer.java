/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/** This deployer is responsible for deploying executions. */
public interface ExecutionDeployer {

    /**
     * Allocate slots and deploy executions.
     *
     * @param executionsToDeploy executions to deploy
     * @param requiredVersionByVertex required versions of the execution vertices. If the actual
     *     version does not match, the deployment of the execution will be rejected.
     */
    void allocateSlotsAndDeploy(
            final List<Execution> executionsToDeploy,
            final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex);

    /** Factory to instantiate the {@link ExecutionDeployer}. */
    interface Factory {

        /**
         * Instantiate an {@link ExecutionDeployer} with the given params. Note that the version of
         * an execution vertex will be recorded before scheduling executions for it. The version may
         * change if a global failure happens, or if the job is canceled, or if the execution vertex
         * is restarted when all its current execution are FAILED/CANCELED. Once the version is
         * changed, the previously triggered execution deployment will be skipped.
         *
         * @param log the logger
         * @param executionSlotAllocator the allocator to allocate slots
         * @param executionOperations the operations of executions
         * @param executionVertexVersioner the versioner which records the versions of execution
         *     vertices.
         * @param partitionRegistrationTimeout timeout of partition registration
         * @param allocationReservationFunc function to reserve allocations for local recovery
         * @param mainThreadExecutor the main thread executor
         * @return an instantiated {@link ExecutionDeployer}
         */
        ExecutionDeployer createInstance(
                final Logger log,
                final ExecutionSlotAllocator executionSlotAllocator,
                final ExecutionOperations executionOperations,
                final ExecutionVertexVersioner executionVertexVersioner,
                final Time partitionRegistrationTimeout,
                final BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc,
                final ComponentMainThreadExecutor mainThreadExecutor);
    }
}
