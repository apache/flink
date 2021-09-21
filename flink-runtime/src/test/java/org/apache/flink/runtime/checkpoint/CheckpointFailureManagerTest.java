/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_EXPIRED;
import static org.apache.flink.runtime.checkpoint.CheckpointProperties.forCheckpoint;
import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
import static org.junit.Assert.assertEquals;

/** Tests for the checkpoint failure manager. */
public class CheckpointFailureManagerTest extends TestLogger {

    @Test
    public void testIgnoresPastCheckpoints() throws IOException, JobException {
        TestFailJobCallback callback = new TestFailJobCallback();
        CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);
        CheckpointProperties checkpointProperties = forCheckpoint(NEVER_RETAIN_AFTER_TERMINATION);

        failureManager.handleJobLevelCheckpointException(
                checkpointProperties, new CheckpointException(CHECKPOINT_EXPIRED), 1L);
        failureManager.handleJobLevelCheckpointException(
                checkpointProperties, new CheckpointException(CHECKPOINT_EXPIRED), 2L);
        failureManager.handleCheckpointSuccess(2L);
        failureManager.handleJobLevelCheckpointException(
                checkpointProperties, new CheckpointException(CHECKPOINT_EXPIRED), 1L);
        failureManager.handleJobLevelCheckpointException(
                checkpointProperties, new CheckpointException(CHECKPOINT_EXPIRED), 3L);
        failureManager.handleJobLevelCheckpointException(
                checkpointProperties, new CheckpointException(CHECKPOINT_EXPIRED), 4L);
        assertEquals(0, callback.getInvokeCounter());
    }

    @Test
    public void testContinuousFailure() throws IOException, JobException {
        TestFailJobCallback callback = new TestFailJobCallback();
        CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);
        CheckpointProperties checkpointProperties = forCheckpoint(NEVER_RETAIN_AFTER_TERMINATION);
        failureManager.handleJobLevelCheckpointException(
                checkpointProperties,
                new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED),
                1);
        failureManager.handleJobLevelCheckpointException(
                checkpointProperties,
                new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED),
                2);

        // ignore this
        failureManager.handleJobLevelCheckpointException(
                checkpointProperties,
                new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION),
                3);

        failureManager.handleJobLevelCheckpointException(
                checkpointProperties,
                new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED),
                4);
        assertEquals(1, callback.getInvokeCounter());
    }

    @Test
    public void testBreakContinuousFailure() throws IOException, JobException {
        TestFailJobCallback callback = new TestFailJobCallback();
        CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);
        CheckpointProperties checkpointProperties = forCheckpoint(NEVER_RETAIN_AFTER_TERMINATION);

        failureManager.handleJobLevelCheckpointException(
                checkpointProperties,
                new CheckpointException(CheckpointFailureReason.IO_EXCEPTION),
                1);
        failureManager.handleJobLevelCheckpointException(
                checkpointProperties,
                new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED),
                2);

        // ignore this
        failureManager.handleJobLevelCheckpointException(
                checkpointProperties,
                new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION),
                3);

        // reset
        failureManager.handleCheckpointSuccess(4);

        failureManager.handleJobLevelCheckpointException(
                checkpointProperties, new CheckpointException(CHECKPOINT_EXPIRED), 5);
        assertEquals(0, callback.getInvokeCounter());
    }

    @Test
    public void testTotalCountValue() throws IOException, JobException {
        TestFailJobCallback callback = new TestFailJobCallback();
        CheckpointProperties checkpointProperties = forCheckpoint(NEVER_RETAIN_AFTER_TERMINATION);
        CheckpointFailureManager failureManager = new CheckpointFailureManager(0, callback);
        for (CheckpointFailureReason reason : CheckpointFailureReason.values()) {
            failureManager.handleJobLevelCheckpointException(
                    checkpointProperties, new CheckpointException(reason), -2);
        }

        // IO_EXCEPTION, CHECKPOINT_DECLINED, CHECKPOINT_EXPIRED and CHECKPOINT_ASYNC_EXCEPTION
        assertEquals(4, callback.getInvokeCounter());
    }

    @Test
    public void testIgnoreOneCheckpointRepeatedlyCountMultiTimes()
            throws IOException, JobException {
        TestFailJobCallback callback = new TestFailJobCallback();
        CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);
        CheckpointProperties checkpointProperties = forCheckpoint(NEVER_RETAIN_AFTER_TERMINATION);

        failureManager.handleJobLevelCheckpointException(
                checkpointProperties,
                new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED),
                1);
        failureManager.handleJobLevelCheckpointException(
                checkpointProperties,
                new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED),
                2);

        // ignore this
        failureManager.handleJobLevelCheckpointException(
                checkpointProperties,
                new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION),
                3);

        // ignore repeatedly report from one checkpoint
        failureManager.handleJobLevelCheckpointException(
                checkpointProperties,
                new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED),
                2);
        assertEquals(0, callback.getInvokeCounter());
    }

    /** A failure handler callback for testing. */
    private static class TestFailJobCallback implements CheckpointFailureManager.FailJobCallback {

        private int invokeCounter = 0;

        @Override
        public void failJob(Throwable cause) {
            invokeCounter++;
        }

        @Override
        public void failJobDueToTaskFailure(
                final Throwable cause, final ExecutionAttemptID executionAttemptID) {
            invokeCounter++;
        }

        public int getInvokeCounter() {
            return invokeCounter;
        }
    }
}
