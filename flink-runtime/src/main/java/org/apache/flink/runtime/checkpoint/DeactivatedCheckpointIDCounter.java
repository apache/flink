/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.api.common.JobStatus;

/**
 * This class represents a {@link CheckpointIDCounter} if checkpointing is deactivated.
 * Consequently, no component should use methods of this class other than {@link #start()} and
 * {@link #shutdown}.
 */
public enum DeactivatedCheckpointIDCounter implements CheckpointIDCounter {
    INSTANCE;

    @Override
    public void start() throws Exception {}

    @Override
    public void shutdown(JobStatus jobStatus) throws Exception {}

    @Override
    public long getAndIncrement() throws Exception {
        throw new UnsupportedOperationException(
                "The DeactivatedCheckpointIDCounter cannot serve checkpoint ids.");
    }

    @Override
    public long get() {
        throw new UnsupportedOperationException(
                "The DeactivatedCheckpointIDCounter cannot serve checkpoint ids.");
    }

    @Override
    public void setCount(long newId) throws Exception {
        throw new UnsupportedOperationException(
                "The DeactivatedCheckpointIDCounter cannot serve checkpoint ids.");
    }
}
