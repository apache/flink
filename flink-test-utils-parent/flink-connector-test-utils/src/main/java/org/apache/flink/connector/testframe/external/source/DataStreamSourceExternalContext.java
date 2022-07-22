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

package org.apache.flink.connector.testframe.external.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.testframe.external.ExternalContext;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;

import java.util.List;

/**
 * External context for DataStream sources.
 *
 * @param <T> Type of elements after deserialization by source
 */
@Experimental
public interface DataStreamSourceExternalContext<T>
        extends ExternalContext, ResultTypeQueryable<T> {

    /**
     * Create an instance of {@link Source} satisfying given options.
     *
     * @param sourceSettings settings of the source
     * @throws UnsupportedOperationException if the provided option is not supported.
     */
    Source<T, ?, ?> createSource(TestingSourceSettings sourceSettings)
            throws UnsupportedOperationException;

    /**
     * Create a new split in the external system and return a data writer corresponding to the new
     * split.
     *
     * @param sourceSettings options of the source
     */
    ExternalSystemSplitDataWriter<T> createSourceSplitDataWriter(
            TestingSourceSettings sourceSettings);

    /**
     * Generate test data.
     *
     * <p>These test data will be written to external system using {@link
     * ExternalSystemSplitDataWriter}, consume back by source in testing Flink job, and make
     * comparison with {@link T#equals(Object)} for validating correctness.
     *
     * <p>Note: Make sure that the {@link T#equals(Object)} returns false when the records in
     * different splits.
     *
     * @param sourceSettings options of the source
     * @param splitIndex index of the split.
     * @param seed Seed for generating random test data set.
     * @return List of generated test data.
     */
    List<T> generateTestData(TestingSourceSettings sourceSettings, int splitIndex, long seed);
}
