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

package org.apache.flink.runtime.accumulators;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * This class encapsulates a map of accumulators for a single task. It is used for the transfer from
 * TaskManagers to the JobManager and from the JobManager to the Client.
 */
public class AccumulatorSnapshot implements Serializable {

    private static final long serialVersionUID = 42L;

    private final JobID jobID;
    private final ExecutionAttemptID executionAttemptID;

    /** Serialized user accumulators which may require the custom user class loader. */
    private final SerializedValue<Map<String, Accumulator<?, ?>>> userAccumulators;

    public AccumulatorSnapshot(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            Map<String, Accumulator<?, ?>> userAccumulators)
            throws IOException {
        this.jobID = jobID;
        this.executionAttemptID = executionAttemptID;
        this.userAccumulators = new SerializedValue<>(userAccumulators);
    }

    public JobID getJobID() {
        return jobID;
    }

    public ExecutionAttemptID getExecutionAttemptID() {
        return executionAttemptID;
    }

    /**
     * Gets the user-defined accumulators values.
     *
     * @return the serialized map
     */
    public Map<String, Accumulator<?, ?>> deserializeUserAccumulators(ClassLoader classLoader)
            throws IOException, ClassNotFoundException {
        return userAccumulators.deserializeValue(classLoader);
    }
}
