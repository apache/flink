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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.jobmanager.ExecutionPlanStoreUtil;

import static org.apache.flink.kubernetes.utils.Constants.EXECUTION_PLAN_STORE_KEY_PREFIX;

/** Singleton {@link ExecutionPlanStoreUtil} implementation for Kubernetes. */
public enum KubernetesExecutionPlanStoreUtil implements ExecutionPlanStoreUtil {
    INSTANCE;

    /**
     * Convert a key in ConfigMap to {@link JobID}. The key is stored with prefix {@link
     * Constants#EXECUTION_PLAN_STORE_KEY_PREFIX}.
     *
     * @param key execution plan key in ConfigMap.
     * @return the parsed {@link JobID}.
     */
    @Override
    public JobID nameToJobID(String key) {
        return JobID.fromHexString(key.substring(EXECUTION_PLAN_STORE_KEY_PREFIX.length()));
    }

    /**
     * Convert a {@link JobID} to config map key. We will add prefix {@link
     * Constants#EXECUTION_PLAN_STORE_KEY_PREFIX}.
     *
     * @param jobID job id
     * @return a key to store execution plan in the ConfigMap
     */
    @Override
    public String jobIDToName(JobID jobID) {
        return EXECUTION_PLAN_STORE_KEY_PREFIX + jobID;
    }
}
