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

package org.apache.flink.runtime.messages.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

/** The base class of all checkpoint messages. */
public abstract class AbstractCheckpointMessage implements java.io.Serializable {

    private static final long serialVersionUID = 186780414819428178L;

    /** The job to which this message belongs */
    private final JobID job;

    /** The task execution that is source/target of the checkpoint message */
    private final ExecutionAttemptID taskExecutionId;

    /** The ID of the checkpoint that this message coordinates */
    private final long checkpointId;

    protected AbstractCheckpointMessage(
            JobID job, ExecutionAttemptID taskExecutionId, long checkpointId) {
        if (job == null || taskExecutionId == null) {
            throw new NullPointerException();
        }

        this.job = job;
        this.taskExecutionId = taskExecutionId;
        this.checkpointId = checkpointId;
    }

    // --------------------------------------------------------------------------------------------

    public JobID getJob() {
        return job;
    }

    public ExecutionAttemptID getTaskExecutionId() {
        return taskExecutionId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return job.hashCode()
                + taskExecutionId.hashCode()
                + (int) (checkpointId ^ (checkpointId >>> 32));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o instanceof AbstractCheckpointMessage) {
            AbstractCheckpointMessage that = (AbstractCheckpointMessage) o;
            return this.job.equals(that.job)
                    && this.taskExecutionId.equals(that.taskExecutionId)
                    && this.checkpointId == that.checkpointId;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "(" + checkpointId + ':' + job + '/' + taskExecutionId + ')';
    }
}
