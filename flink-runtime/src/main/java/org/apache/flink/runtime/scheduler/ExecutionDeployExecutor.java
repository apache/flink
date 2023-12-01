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
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.util.concurrent.ScheduledExecutor;

/** Executor to deploy {@link Execution}. */
public interface ExecutionDeployExecutor {

    /**
     * Deploy the execution.
     *
     * @param execution to deploy.
     * @throws JobException if the execution cannot be deployed to the assigned resource
     */
    void executeDeploy(Execution execution) throws JobException;

    /** Flush all execution deployment. */
    void flushDeploy();

    /** Instantiate an {@link ExecutionDeployExecutor} with the given params. */
    interface Factory {

        /**
         * Instantiate an {@link ExecutionDeployExecutor} with the given params.
         *
         * @param executionOperations the operations of executions.
         * @param scheduledExecutor executor to be used for deploying execution.
         * @param rpcTimeout timeout of deploy request.
         * @return an instantiated {@link ExecutionDeployExecutor}
         */
        ExecutionDeployExecutor createInstance(
                ExecutionOperations executionOperations,
                ScheduledExecutor scheduledExecutor,
                Time rpcTimeout);
    }
}
