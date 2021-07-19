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

package org.apache.flink.connectors.test.common.external;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;

import java.io.Serializable;
import java.util.Collection;

/**
 * Context of the test interacting with external system.
 *
 * <p>User need to provide some instances and information of the test to testing framework,
 * including name of Flink jobs, instance of tested source/sink and job termination pattern.
 *
 * @param <T> Type of elements after deserialization by source and before serialization by sink
 */
public interface ExternalContext<T> extends Serializable, AutoCloseable {

    /**
     * Create a new instance of connector source implemented in {@link Source}.
     *
     * @return A new instance of Source
     */
    Source<T, ?, ?> createSource(Boundedness boundedness);

    /**
     * Create a new instance of connector sink implemented in {@link Sink}.
     *
     * @return A new instance of Sink
     */
    Sink<T, ?, ?, ?> createSink();

    SourceSplitDataWriter<T> createSourceSplit();

    Collection<T> generateTestRecords();
}
