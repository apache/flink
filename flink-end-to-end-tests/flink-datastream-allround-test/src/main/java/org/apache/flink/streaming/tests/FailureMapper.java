/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;

/**
 * This mapper simulates failure by throwing exceptions. The timing to throw an exception by
 * configuring the number of records to process, and number of complete checkpoints to be
 * acknowledged before throwing the exception.
 *
 * <p>The total times to simulate a failure across multiple execution attempts of the operator can
 * also be configured. Note that this also takes into account failures that were not triggered by
 * this mapper, e.g. TaskManager failures.
 */
public class FailureMapper<T> extends RichMapFunction<T, T> implements CheckpointListener {

    private static final long serialVersionUID = -5286927943454740016L;

    private final long numProcessedRecordsFailureThreshold;
    private final long numCompleteCheckpointsFailureThreshold;
    private final int maxNumFailures;

    private long numProcessedRecords;
    private long numCompleteCheckpoints;

    public FailureMapper(
            long numProcessedRecordsFailureThreshold,
            long numCompleteCheckpointsFailureThreshold,
            int maxNumFailures) {

        this.numProcessedRecordsFailureThreshold = numProcessedRecordsFailureThreshold;
        this.numCompleteCheckpointsFailureThreshold = numCompleteCheckpointsFailureThreshold;
        this.maxNumFailures = maxNumFailures;
    }

    @Override
    public T map(T value) throws Exception {
        numProcessedRecords++;

        if (isReachedFailureThreshold()) {
            throw new Exception("Artificial failure.");
        }

        return value;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        numCompleteCheckpoints++;

        if (isReachedFailureThreshold()) {
            throw new Exception("Artificial failure.");
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {}

    private boolean isReachedFailureThreshold() {
        return numProcessedRecords >= numProcessedRecordsFailureThreshold
                && numCompleteCheckpoints >= numCompleteCheckpointsFailureThreshold
                && getRuntimeContext().getAttemptNumber() < maxNumFailures;
    }
}
