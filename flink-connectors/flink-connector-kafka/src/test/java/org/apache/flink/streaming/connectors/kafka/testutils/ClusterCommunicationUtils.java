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

package org.apache.flink.streaming.connectors.kafka.testutils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.client.JobStatusMessage;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/** Utilities for communicating with a cluster through a {@link ClusterClient}. */
public class ClusterCommunicationUtils {

    public static void waitUntilJobIsRunning(ClusterClient<?> client) throws Exception {
        while (getRunningJobs(client).isEmpty()) {
            Thread.sleep(50);
        }
    }

    public static void waitUntilNoJobIsRunning(ClusterClient<?> client) throws Exception {
        while (!getRunningJobs(client).isEmpty()) {
            Thread.sleep(50);
        }
    }

    public static List<JobID> getRunningJobs(ClusterClient<?> client) throws Exception {
        Collection<JobStatusMessage> statusMessages = client.listJobs().get();
        return statusMessages.stream()
                .filter(status -> !status.getJobState().isGloballyTerminalState())
                .map(JobStatusMessage::getJobId)
                .collect(Collectors.toList());
    }

    private ClusterCommunicationUtils() {}
}
