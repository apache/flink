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

package org.apache.flink.table.connector.format;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A {@link Format} for a {@link DynamicTableSink} for writing rows.
 *
 * @param <I> runtime interface needed by the table sink
 */
@PublicEvolving
public interface EncodingFormat<I> extends Format {

    /**
     * Creates runtime encoder implementation that is configured to consume data of the given data
     * type.
     */
    I createRuntimeEncoder(DynamicTableSink.Context context, DataType physicalDataType);

    /**
     * Returns the map of metadata keys and their corresponding data types that can be consumed by
     * this format for writing. By default, this method returns an empty map.
     *
     * <p>Metadata columns add additional columns to the table's schema. An encoding format is
     * responsible to accept requested metadata columns at the end of consumed rows and persist
     * them.
     *
     * <p>See {@link SupportsWritingMetadata} for more information.
     *
     * <p>Note: This method is only used if the outer {@link DynamicTableSink} implements {@link
     * SupportsWritingMetadata} and calls this method in {@link
     * SupportsWritingMetadata#listWritableMetadata()}.
     */
    default Map<String, DataType> listWritableMetadata() {
        return Collections.emptyMap();
    }

    /**
     * Provides a list of metadata keys that the consumed row will contain as appended metadata
     * columns. By default, this method throws an exception if metadata keys are defined.
     *
     * <p>See {@link SupportsWritingMetadata} for more information.
     *
     * <p>Note: This method is only used if the outer {@link DynamicTableSink} implements {@link
     * SupportsWritingMetadata} and calls this method in {@link
     * SupportsWritingMetadata#applyWritableMetadata(List, DataType)}.
     */
    @SuppressWarnings("unused")
    default void applyWritableMetadata(List<String> metadataKeys) {
        throw new UnsupportedOperationException(
                "An encoding format must override this method to apply metadata keys.");
    }
}
