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

/** Default implementation of {@link ExecutionDeployExecutor}. */
public class DefaultExecutionDeployExecutor implements ExecutionDeployExecutor {

    protected ExecutionOperations executionOperations;

    public DefaultExecutionDeployExecutor(ExecutionOperations executionOperations) {
        this.executionOperations = executionOperations;
    }

    @Override
    public void executeDeploy(Execution execution) throws JobException {
        executionOperations.deploy(execution);
    }

    @Override
    public void flushDeploy() {
        // do nothing.
    }

    /** Factory to instantiate the {@link DefaultExecutionDeployExecutor}. */
    public static class Factory implements ExecutionDeployExecutor.Factory {

        @Override
        public ExecutionDeployExecutor createInstance(
                ExecutionOperations executionOperations,
                ScheduledExecutor scheduledExecutor,
                Time rpcTimeout) {
            return new DefaultExecutionDeployExecutor(executionOperations);
        }
    }
}
