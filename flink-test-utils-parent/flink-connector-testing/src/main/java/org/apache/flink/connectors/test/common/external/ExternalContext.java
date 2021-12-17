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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;

import java.io.Serializable;
import java.util.List;

/**
 * Context of the test interacting with external system.
 *
 * <p>This context is responsible for providing:
 *
 * <ul>
 *   <li>Instance of sources connecting to external system
 *   <li>{@link SourceSplitDataWriter} for creating splits and writing test data into the created
 *       split
 *   <li>Test data to write into the external system
 * </ul>
 *
 * @param <T> Type of elements after deserialization by source
 */
@Experimental
public interface ExternalContext<T> extends Serializable, AutoCloseable {

    /**
     * Create a new instance of connector source implemented in {@link Source}.
     *
     * @return A new instance of Source
     */
    Source<T, ?, ?> createSource(Boundedness boundedness);

    /**
     * Create a new split in the external system and a data writer corresponding to the new split.
     *
     * @return A data writer for the created split.
     */
    SourceSplitDataWriter<T> createSourceSplitDataWriter();

    /**
     * Generate test data.
     *
     * <p>Make sure that the {@link T#equals(Object)} returns false when the records in different
     * splits.
     *
     * @param splitIndex index of the split.
     * @param seed Seed for generating random test data set.
     * @return List of generated test data.
     */
    List<T> generateTestData(int splitIndex, long seed);

    /**
     * Factory for {@link ExternalContext}.
     *
     * @param <T> Type of elements after deserialization by source
     */
    interface Factory<T> {
        ExternalContext<T> createExternalContext();
    }
}
