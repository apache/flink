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
 *
 */

package org.apache.flink.api.connector.sink;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * This interface lets the sink developer build a simple sink topology, which could guarantee the
 * exactly once semantics in both batch and stream execution mode if there is a {@link Committer} or
 * {@link GlobalCommitter}. 1. The {@link SinkWriter} is responsible for producing the committable.
 * 2. The {@link Committer} is responsible for committing a single committable. 3. The {@link
 * GlobalCommitter} is responsible for committing an aggregated committable, which we call the
 * global committable. The {@link GlobalCommitter} is always executed with a parallelism of 1. Note:
 * Developers need to ensure the idempotence of {@link Committer} and {@link GlobalCommitter}.
 *
 * <p>A sink must always have a writer, but committer and global committer are each optional and all
 * combinations are valid.
 *
 * <p>The {@link Sink} needs to be serializable. All configuration should be validated eagerly. The
 * respective sink parts are transient and will only be created in the subtasks on the taskmanagers.
 *
 * @param <InputT> The type of the sink's input
 * @param <CommT> The type of information needed to commit data staged by the sink
 * @param <WriterStateT> The type of the sink writer's state
 * @param <GlobalCommT> The type of the aggregated committable
 */
@Experimental
public interface Sink<InputT, CommT, WriterStateT, GlobalCommT> extends Serializable {

    /**
     * Create a {@link SinkWriter}. If the application is resumed from a checkpoint or savepoint and
     * the sink is stateful, it will receive the corresponding state obtained with {@link
     * SinkWriter#snapshotState(long)} and serialized with {@link #getWriterStateSerializer()}. If
     * no state exists, the first existing, compatible state specified in {@link
     * #getCompatibleStateNames()} will be loaded and passed.
     *
     * @param context the runtime context.
     * @param states the writer's previous state.
     * @return A sink writer.
     * @throws IOException for any failure during creation.
     * @see SinkWriter#snapshotState(long)
     * @see #getWriterStateSerializer()
     * @see #getCompatibleStateNames()
     */
    SinkWriter<InputT, CommT, WriterStateT> createWriter(
            InitContext context, List<WriterStateT> states) throws IOException;

    /**
     * Any stateful sink needs to provide this state serializer and implement {@link
     * SinkWriter#snapshotState(long)} properly. The respective state is used in {@link
     * #createWriter(InitContext, List)} on recovery.
     *
     * @return the serializer of the writer's state type.
     */
    Optional<SimpleVersionedSerializer<WriterStateT>> getWriterStateSerializer();

    /**
     * Creates a {@link Committer} which is part of a 2-phase-commit protocol. The {@link
     * SinkWriter} creates committables through {@link SinkWriter#prepareCommit(boolean)} in the
     * first phase. The committables are then passed to this committer and persisted with {@link
     * Committer#commit(List)}. If a committer is returned, the sink must also return a {@link
     * #getCommittableSerializer()}.
     *
     * @return A committer for the 2-phase-commit protocol.
     * @throws IOException for any failure during creation.
     */
    Optional<Committer<CommT>> createCommitter() throws IOException;

    /**
     * Creates a {@link GlobalCommitter} which is part of a 2-phase-commit protocol. The {@link
     * SinkWriter} creates committables through {@link SinkWriter#prepareCommit(boolean)} in the
     * first phase. The committables are then passed to the Committer and persisted with {@link
     * Committer#commit(List)}. The committables are also passed to this {@link GlobalCommitter} of
     * which only a single instance exists. If a global committer is returned, the sink must also
     * return a {@link #getCommittableSerializer()} and {@link #getGlobalCommittableSerializer()}.
     *
     * @return A global committer for the 2-phase-commit protocol.
     * @throws IOException for any failure during creation.
     */
    Optional<GlobalCommitter<CommT, GlobalCommT>> createGlobalCommitter() throws IOException;

    /**
     * Returns the serializer of the committable type. The serializer is required iff the sink has a
     * {@link Committer} or {@link GlobalCommitter}.
     */
    Optional<SimpleVersionedSerializer<CommT>> getCommittableSerializer();

    /**
     * Returns the serializer of the aggregated committable type. The serializer is required iff the
     * sink has a {@link GlobalCommitter}.
     */
    Optional<SimpleVersionedSerializer<GlobalCommT>> getGlobalCommittableSerializer();

    /**
     * A list of state names of sinks from which the state can be restored. For example, the new
     * {@code FileSink} can resume from the state of an old {@code StreamingFileSink} as a drop-in
     * replacement when resuming from a checkpoint/savepoint.
     */
    default Collection<String> getCompatibleStateNames() {
        return Collections.emptyList();
    }

    /** The interface exposes some runtime info for creating a {@link SinkWriter}. */
    interface InitContext {
        /**
         * Gets the {@link UserCodeClassLoader} to load classes that are not in system's classpath,
         * but are part of the jar file of a user job.
         *
         * @see UserCodeClassLoader
         */
        UserCodeClassLoader getUserCodeClassLoader();

        /**
         * Returns the mailbox executor that allows to execute {@link Runnable}s inside the task
         * thread in between record processing.
         *
         * <p>Note that this method should not be used per-record for performance reasons in the
         * same way as records should not be sent to the external system individually. Rather,
         * implementers are expected to batch records and only enqueue a single {@link Runnable} per
         * batch to handle the result.
         */
        MailboxExecutor getMailboxExecutor();

        /**
         * Returns a {@link ProcessingTimeService} that can be used to get the current time and
         * register timers.
         */
        ProcessingTimeService getProcessingTimeService();

        /** @return The id of task where the writer is. */
        int getSubtaskId();

        /** @return The number of parallel Sink tasks. */
        int getNumberOfParallelSubtasks();

        /** @return The metric group this writer belongs to. */
        SinkWriterMetricGroup metricGroup();

        /**
         * Returns id of the restored checkpoint, if state was restored from the snapshot of a
         * previous execution.
         */
        OptionalLong getRestoredCheckpointId();
    }

    /**
     * A service that allows to get the current processing time and register timers that will
     * execute the given {@link ProcessingTimeCallback} when firing.
     */
    interface ProcessingTimeService {

        /** Returns the current processing time. */
        long getCurrentProcessingTime();

        /**
         * Invokes the given callback at the given timestamp.
         *
         * @param time Time when the callback is invoked at
         * @param processingTimerCallback The callback to be invoked.
         */
        void registerProcessingTimer(long time, ProcessingTimeCallback processingTimerCallback);

        /**
         * A callback that can be registered via {@link #registerProcessingTimer(long,
         * ProcessingTimeCallback)}.
         */
        interface ProcessingTimeCallback {

            /**
             * This method is invoked with the time which the callback register for.
             *
             * @param time The time this callback was registered for.
             */
            void onProcessingTime(long time) throws IOException, InterruptedException;
        }
    }
}
