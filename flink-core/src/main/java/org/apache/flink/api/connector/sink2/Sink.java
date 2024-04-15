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

package org.apache.flink.api.connector.sink2;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

/**
 * Base interface for developing a sink. A basic {@link Sink} is a stateless sink that can flush
 * data on checkpoint to achieve at-least-once consistency. Sinks with additional requirements
 * should implement {@link SupportsWriterState} or {@link SupportsCommitter}.
 *
 * <p>The {@link Sink} needs to be serializable. All configuration should be validated eagerly. The
 * respective sink writers are transient and will only be created in the subtasks on the
 * taskmanagers.
 *
 * @param <InputT> The type of the sink's input
 */
@PublicEvolving
public interface Sink<InputT> extends Serializable {

    /**
     * Creates a {@link SinkWriter}.
     *
     * @param context the runtime context.
     * @return A sink writer.
     * @throws IOException for any failure during creation.
     * @deprecated Please implement {@link #createWriter(WriterInitContext)}. For backward
     *     compatibility reasons - to keep {@link Sink} a functional interface - Flink did not
     *     provide a default implementation. New {@link Sink} implementations should implement this
     *     method, but it will not be used, and it will be removed in 1.20.0 release. Do not use
     *     {@link Override} annotation when implementing this method, to prevent compilation errors
     *     when migrating to 1.20.x release.
     */
    @Deprecated
    SinkWriter<InputT> createWriter(InitContext context) throws IOException;

    /**
     * Creates a {@link SinkWriter}.
     *
     * @param context the runtime context.
     * @return A sink writer.
     * @throws IOException for any failure during creation.
     */
    default SinkWriter<InputT> createWriter(WriterInitContext context) throws IOException {
        return createWriter(new InitContextWrapper(context));
    }

    /** The interface exposes some runtime info for creating a {@link SinkWriter}. */
    @PublicEvolving
    @Deprecated
    interface InitContext extends org.apache.flink.api.connector.sink2.InitContext {
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

        /** @return The metric group this writer belongs to. */
        SinkWriterMetricGroup metricGroup();

        /**
         * Provides a view on this context as a {@link SerializationSchema.InitializationContext}.
         */
        SerializationSchema.InitializationContext asSerializationSchemaInitializationContext();

        /** Returns whether object reuse has been enabled or disabled. */
        boolean isObjectReuseEnabled();

        /** Creates a serializer for the type of sink's input. */
        <IN> TypeSerializer<IN> createInputSerializer();

        /**
         * Returns a metadata consumer, the {@link SinkWriter} can publish metadata events of type
         * {@link MetaT} to the consumer.
         *
         * <p>It is recommended to use a separate thread pool to publish the metadata because
         * enqueuing a lot of these messages in the mailbox may lead to a performance decrease.
         * thread, and the {@link Consumer#accept} method is executed very fast.
         */
        @Experimental
        default <MetaT> Optional<Consumer<MetaT>> metadataConsumer() {
            return Optional.empty();
        }
    }

    /**
     * Class for wrapping a new {@link WriterInitContext} to an old {@link InitContext} until
     * deprecation.
     *
     * @deprecated Internal, do not use it.
     */
    @Deprecated
    class InitContextWrapper implements InitContext {
        private final WriterInitContext wrapped;

        public InitContextWrapper(WriterInitContext wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public int getSubtaskId() {
            return wrapped.getSubtaskId();
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return wrapped.getNumberOfParallelSubtasks();
        }

        @Override
        public int getAttemptNumber() {
            return wrapped.getAttemptNumber();
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return wrapped.getRestoredCheckpointId();
        }

        @Override
        public JobID getJobId() {
            return wrapped.getJobId();
        }

        @Override
        public JobInfo getJobInfo() {
            return wrapped.getJobInfo();
        }

        @Override
        public TaskInfo getTaskInfo() {
            return wrapped.getTaskInfo();
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return wrapped.getUserCodeClassLoader();
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return wrapped.getMailboxExecutor();
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return wrapped.getProcessingTimeService();
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return wrapped.metricGroup();
        }

        @Override
        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            return wrapped.asSerializationSchemaInitializationContext();
        }

        @Override
        public boolean isObjectReuseEnabled() {
            return wrapped.isObjectReuseEnabled();
        }

        @Override
        public <IN> TypeSerializer<IN> createInputSerializer() {
            return wrapped.createInputSerializer();
        }

        @Experimental
        @Override
        public <MetaT> Optional<Consumer<MetaT>> metadataConsumer() {
            return wrapped.metadataConsumer();
        }
    }
}
