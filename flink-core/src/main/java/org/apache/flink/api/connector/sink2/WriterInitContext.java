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

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.Optional;
import java.util.function.Consumer;

/** The interface exposes some runtime info for creating a {@link SinkWriter}. */
@Public
public interface WriterInitContext extends org.apache.flink.api.connector.sink2.InitContext {
    /**
     * Gets the {@link UserCodeClassLoader} to load classes that are not in system's classpath, but
     * are part of the jar file of a user job.
     *
     * @see UserCodeClassLoader
     */
    UserCodeClassLoader getUserCodeClassLoader();

    /**
     * Returns the mailbox executor that allows to execute {@link Runnable}s inside the task thread
     * in between record processing.
     *
     * <p>Note that this method should not be used per-record for performance reasons in the same
     * way as records should not be sent to the external system individually. Rather, implementers
     * are expected to batch records and only enqueue a single {@link Runnable} per batch to handle
     * the result.
     */
    MailboxExecutor getMailboxExecutor();

    /**
     * Returns a {@link ProcessingTimeService} that can be used to get the current time and register
     * timers.
     */
    ProcessingTimeService getProcessingTimeService();

    /** @return The metric group this writer belongs to. */
    SinkWriterMetricGroup metricGroup();

    /** Provides a view on this context as a {@link SerializationSchema.InitializationContext}. */
    SerializationSchema.InitializationContext asSerializationSchemaInitializationContext();

    /** Returns whether object reuse has been enabled or disabled. */
    boolean isObjectReuseEnabled();

    /** Creates a serializer for the type of sink's input. */
    <IN> TypeSerializer<IN> createInputSerializer();

    /**
     * Returns a metadata consumer, the {@link SinkWriter} can publish metadata events of type
     * {@link MetaT} to the consumer.
     *
     * <p>It is recommended to use a separate thread pool to publish the metadata because enqueuing
     * a lot of these messages in the mailbox may lead to a performance decrease. thread, and the
     * {@link Consumer#accept} method is executed very fast.
     */
    default <MetaT> Optional<Consumer<MetaT>> metadataConsumer() {
        return Optional.empty();
    }
}
