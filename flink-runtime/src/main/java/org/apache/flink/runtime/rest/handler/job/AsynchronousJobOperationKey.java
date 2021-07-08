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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.rest.handler.async.AbstractAsynchronousOperationHandlers;
import org.apache.flink.runtime.rest.handler.async.OperationKey;
import org.apache.flink.runtime.rest.messages.TriggerId;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A pair of {@link JobID} and {@link TriggerId} used as a key to a hash based collection.
 *
 * @see AbstractAsynchronousOperationHandlers
 */
@Immutable
public class AsynchronousJobOperationKey extends OperationKey {

    private final JobID jobId;

    private AsynchronousJobOperationKey(final TriggerId triggerId, final JobID jobId) {
        super(triggerId);
        this.jobId = requireNonNull(jobId);
    }

    public static AsynchronousJobOperationKey of(final TriggerId triggerId, final JobID jobId) {
        return new AsynchronousJobOperationKey(triggerId, jobId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        AsynchronousJobOperationKey that = (AsynchronousJobOperationKey) o;
        return Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), jobId);
    }
}
