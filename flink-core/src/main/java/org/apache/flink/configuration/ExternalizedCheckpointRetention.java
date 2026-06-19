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

package org.apache.flink.configuration;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/** Cleanup behaviour for externalized checkpoints when the job is cancelled. */
@PublicEvolving
public enum ExternalizedCheckpointRetention implements DescribedEnum {

    /**
     * Delete externalized checkpoints on job cancellation.
     *
     * <p>All checkpoint state will be deleted when you cancel the owning job, both the meta data
     * and actual program state. Therefore, you cannot resume from externalized checkpoints after
     * the job has been cancelled.
     *
     * <p>Note that checkpoint state is always kept if the job terminates with state {@link
     * JobStatus#FAILED}.
     */
    DELETE_ON_CANCELLATION(
            text(
                    "Checkpoint state is only kept when the owning job fails. It is deleted if "
                            + "the job is cancelled.")),

    /**
     * Retain externalized checkpoints on job cancellation.
     *
     * <p>All checkpoint state is kept when you cancel the owning job. You have to manually delete
     * both the checkpoint meta data and actual program state after cancelling the job.
     *
     * <p>Note that checkpoint state is always kept if the job terminates with state {@link
     * JobStatus#FAILED}.
     */
    RETAIN_ON_CANCELLATION(
            text("Checkpoint state is kept when the owning job is cancelled or fails.")),

    /** Externalized checkpoints are disabled completely. */
    NO_EXTERNALIZED_CHECKPOINTS(text("Externalized checkpoints are disabled."));

    private final InlineElement description;

    ExternalizedCheckpointRetention(InlineElement description) {
        this.description = description;
    }

    /**
     * Returns whether persistent checkpoints shall be discarded on cancellation of the job.
     *
     * @return <code>true</code> if persistent checkpoints shall be discarded on cancellation of the
     *     job.
     */
    public boolean deleteOnCancellation() {
        return this == DELETE_ON_CANCELLATION;
    }

    @Override
    @Internal
    public InlineElement getDescription() {
        return description;
    }
}
