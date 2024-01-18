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

package org.apache.flink.connector.datagen.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceReaderFactory;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource.NumberSequenceSplit;
import org.apache.flink.connector.datagen.functions.IndexLookupGeneratorFunction;

import java.util.Collection;
import java.util.function.BooleanSupplier;

/** A collection of factory methods for creating data generator-based sources. */
@Experimental
public class TestDataGenerators {

    /**
     * Creates a source that emits provided {@code data}, waits for two checkpoints and emits the
     * same {@code data } again. It is intended only to be used for test purposes. See {@link
     * DoubleEmittingSourceReaderWithCheckpointsInBetween} for details.
     *
     * @param typeInfo The type information of the elements.
     * @param data The collection of elements to create the stream from.
     * @param <OUT> The type of the returned data stream.
     */
    public static <OUT> DataGeneratorSource<OUT> fromDataWithSnapshotsLatch(
            Collection<OUT> data, TypeInformation<OUT> typeInfo) {
        IndexLookupGeneratorFunction<OUT> generatorFunction =
                new IndexLookupGeneratorFunction<>(typeInfo, data);

        return new DataGeneratorSource<>(
                (SourceReaderFactory<OUT, NumberSequenceSplit>)
                        (readerContext) ->
                                new DoubleEmittingSourceReaderWithCheckpointsInBetween<>(
                                        readerContext, generatorFunction),
                generatorFunction,
                data.size(),
                typeInfo);
    }

    /**
     * Creates a source that emits provided {@code data}, waits for two checkpoints and emits the
     * same {@code data } again. It is intended only to be used for test purposes. See {@link
     * DoubleEmittingSourceReaderWithCheckpointsInBetween} for details.
     *
     * @param typeInfo The type information of the elements.
     * @param data The collection of elements to create the stream from.
     * @param <OUT> The type of the returned data stream.
     * @param allowedToExit The boolean supplier that makes it possible to delay termination based
     *     on external conditions.
     */
    public static <OUT> DataGeneratorSource<OUT> fromDataWithSnapshotsLatch(
            Collection<OUT> data, TypeInformation<OUT> typeInfo, BooleanSupplier allowedToExit) {
        IndexLookupGeneratorFunction<OUT> generatorFunction =
                new IndexLookupGeneratorFunction<>(typeInfo, data);

        return new DataGeneratorSource<>(
                (SourceReaderFactory<OUT, NumberSequenceSplit>)
                        (readerContext) ->
                                new DoubleEmittingSourceReaderWithCheckpointsInBetween<>(
                                        readerContext, generatorFunction, allowedToExit),
                generatorFunction,
                data.size(),
                typeInfo);
    }
}
