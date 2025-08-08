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

package org.apache.flink.runtime.util;

public class ContextGetter {
    /**
     * Returns a context string that includes the job ID and task name, along with environment
     * information to identify the execution context (e.g., container ID or pod name).
     *
     * @param jobId The job ID of the Flink job.
     * @param taskName The name of the task.
     * @return A context string formatted with job ID and task name.
     */
    public static String getContext(String jobId, String taskName) {

        String context = "FLINK";
        if (System.getenv().get("CONTAINER_ID") != null) {
            String[] application = System.getenv().get("CONTAINER_ID").split("_");
            context = context + "_application_" + application[2] + "_" + application[3];
        } else if (System.getenv().get("KUBERNETES_SERVICE_HOST") != null) {
            String podName = System.getenv("HOSTNAME");

            context = context + "_pod_" + podName;

        } else {
            context = context + "_local";
        }
        context = context + "_JobID_" + jobId + "_TaskName_" + taskName;
        return context;
    }
}
