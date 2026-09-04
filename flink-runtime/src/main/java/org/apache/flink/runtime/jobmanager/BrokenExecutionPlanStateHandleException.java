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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.util.FlinkException;

/**
 * Thrown by {@link ExecutionPlanStore#recoverExecutionPlan(JobID)} when a job's persisted {@link
 * org.apache.flink.streaming.api.graph.ExecutionPlan} cannot be deserialized because its state
 * handle is broken (e.g. the backing file is missing/corrupted, or written by an incompatible Flink
 * version). Unlike other recovery failures (e.g. a temporarily unreachable backend), this is not
 * transient, so callers can react by skipping just the affected job instead of failing the whole
 * recovery.
 */
public class BrokenExecutionPlanStateHandleException extends FlinkException {

    private static final long serialVersionUID = 1L;

    public BrokenExecutionPlanStateHandleException(String message, Throwable cause) {
        super(message, cause);
    }
}
