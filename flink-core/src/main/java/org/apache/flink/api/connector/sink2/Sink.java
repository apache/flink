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

import java.io.IOException;
import java.io.Serializable;

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
@Public
public interface Sink<InputT> extends Serializable {

    /**
     * Creates a {@link SinkWriter}.
     *
     * @param context the runtime context.
     * @return A sink writer.
     * @throws IOException for any failure during creation.
     */
    SinkWriter<InputT> createWriter(WriterInitContext context) throws IOException;
}
