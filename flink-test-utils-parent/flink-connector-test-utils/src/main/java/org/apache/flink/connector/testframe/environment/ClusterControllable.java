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

package org.apache.flink.connector.testframe.environment;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.core.execution.JobClient;

/** Interface for triggering failover in a Flink cluster. */
@Experimental
public interface ClusterControllable {

    /**
     * Triggers a JobManager failover.
     *
     * @param jobClient client of the running job
     * @param afterFailAction action to take before restarting the JobManager
     */
    void triggerJobManagerFailover(JobClient jobClient, Runnable afterFailAction) throws Exception;

    /**
     * Triggers TaskManager failover.
     *
     * @param jobClient client of the running job
     * @param afterFailAction action to take before restarting TaskManager(s)
     */
    void triggerTaskManagerFailover(JobClient jobClient, Runnable afterFailAction) throws Exception;

    /**
     * Disconnect network between Flink cluster and external system.
     *
     * @param jobClient client of the running job
     * @param afterFailAction action to take before recovering the network connection
     */
    void isolateNetwork(JobClient jobClient, Runnable afterFailAction) throws Exception;
}
