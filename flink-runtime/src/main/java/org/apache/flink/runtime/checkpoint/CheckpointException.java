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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.util.Preconditions;

/** Base class for checkpoint related exceptions. */
public class CheckpointException extends Exception {

    private static final long serialVersionUID = 3257526119022486948L;

    private final CheckpointFailureReason checkpointFailureReason;

    public CheckpointException(CheckpointFailureReason failureReason) {
        super(failureReason.message());
        this.checkpointFailureReason = Preconditions.checkNotNull(failureReason);
    }

    public CheckpointException(String message, CheckpointFailureReason failureReason) {
        super(message + " Failure reason: " + failureReason.message());
        this.checkpointFailureReason = Preconditions.checkNotNull(failureReason);
    }

    public CheckpointException(CheckpointFailureReason failureReason, Throwable cause) {
        super(failureReason.message(), cause);
        this.checkpointFailureReason = Preconditions.checkNotNull(failureReason);
    }

    public CheckpointException(
            String message, CheckpointFailureReason failureReason, Throwable cause) {
        super(message + " Failure reason: " + failureReason.message(), cause);
        this.checkpointFailureReason = Preconditions.checkNotNull(failureReason);
    }

    public CheckpointFailureReason getCheckpointFailureReason() {
        return checkpointFailureReason;
    }
}
