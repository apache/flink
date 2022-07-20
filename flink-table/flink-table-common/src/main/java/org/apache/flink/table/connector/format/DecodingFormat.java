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
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A {@link Format} for a {@link DynamicTableSource} for reading rows.
 *
 * <h1>Implementing a {@link DecodingFormat}</h1>
 *
 * {@link DecodingFormat#createRuntimeDecoder(DynamicTableSource.Context, DataType)} takes a {@code
 * physicalDataType}. This {@link DataType} has usually been derived from a table's {@link
 * ResolvedSchema} and excludes partition, metadata, and other auxiliary columns. The {@code
 * physicalDataType} should describe exactly the full serialized record. In other words: for every
 * field in the serialized record there is a corresponding field at the same position in the {@code
 * physicalDataType}. Some implementations may decide to be more lenient and allow users to omit
 * fields but this depends on the format characteristics. For example, a CSV format implementation
 * might allow the user to define the schema only for the first 5 of the 10 total columns available
 * in each row.
 *
 * <p>If the format supports projections, that is it can exclude certain fields from being parsed
 * <b>independently of the fields defined in the schema</b> and <b>can reorder fields</b> in the
 * produced {@link RowData}, then it should implement {@link ProjectableDecodingFormat}. {@link
 * ProjectableDecodingFormat#createRuntimeDecoder(DynamicTableSource.Context, DataType, int[][])}
 * provides the {@code physicalDataType} as described above and provides {@code projections} to
 * compute the type to produce using {@code Projection.of(projections).project(physicalDataType)}.
 * For example, a JSON format implementation may match the fields based on the JSON object keys,
 * hence it can easily produce {@link RowData} excluding unused object values and set values inside
 * the {@link RowData} using the index provided by the {@code projections} array.
 *
 * <p>Whenever possible, it's highly recommended implementing {@link ProjectableDecodingFormat}, as
 * it might help to reduce the data volume when users are reading large records but are using only a
 * small subset of fields.
 *
 * <h1>Using a {@link DecodingFormat}</h1>
 *
 * {@link DynamicTableSource} that doesn't implement {@link SupportsProjectionPushDown} should
 * invoke {@link DecodingFormat#createRuntimeDecoder(DynamicTableSource.Context, DataType)}.
 * Usually, {@link DynamicTableFactory.Context#getPhysicalRowDataType()} can provide the {@code
 * physicalDataType} (stripped of any fields not available in the serialized record).
 *
 * <p>{@link DynamicTableSource} implementing {@link SupportsProjectionPushDown} should check
 * whether the {@link DecodingFormat} is an instance of {@link ProjectableDecodingFormat}:
 *
 * <ul>
 *   <li>If yes, then the connector can invoke {@link
 *       ProjectableDecodingFormat#createRuntimeDecoder(DynamicTableSource.Context, DataType,
 *       int[][])} providing a non null {@code projections} array excluding auxiliary fields. The
 *       built runtime implementation will take care of projections, producing records of type
 *       {@code Projection.of(projections).project(physicalDataType)}.
 *   <li>If no, then the connector must take care of performing the projection, for example using
 *       {@link ProjectedRowData} to project physical {@link RowData} emitted from the decoder
 *       runtime implementation.
 * </ul>
 *
 * @param <I> runtime interface needed by the table source
 */
@PublicEvolving
public interface DecodingFormat<I> extends Format {

    /**
     * Creates runtime decoder implementation that is configured to produce data of the given data
     * type.
     *
     * @param context the context provides several utilities required to instantiate the runtime
     *     decoder implementation of the format
     * @param physicalDataType For more details check the documentation of {@link DecodingFormat}.
     */
    I createRuntimeDecoder(DynamicTableSource.Context context, DataType physicalDataType);

    /**
     * Returns the map of metadata keys and their corresponding data types that can be produced by
     * this format for reading. By default, this method returns an empty map.
     *
     * <p>Metadata columns add additional columns to the table's schema. A decoding format is
     * responsible to add requested metadata columns at the end of produced rows.
     *
     * <p>See {@link SupportsReadingMetadata} for more information.
     *
     * <p>Note: This method is only used if the outer {@link DynamicTableSource} implements {@link
     * SupportsReadingMetadata} and calls this method in {@link
     * SupportsReadingMetadata#listReadableMetadata()}.
     */
    default Map<String, DataType> listReadableMetadata() {
        return Collections.emptyMap();
    }

    /**
     * Provides a list of metadata keys that the produced row must contain as appended metadata
     * columns. By default, this method throws an exception if metadata keys are defined.
     *
     * <p>See {@link SupportsReadingMetadata} for more information.
     *
     * <p>Note: This method is only used if the outer {@link DynamicTableSource} implements {@link
     * SupportsReadingMetadata} and calls this method in {@link
     * SupportsReadingMetadata#applyReadableMetadata(List, DataType)}.
     */
    @SuppressWarnings("unused")
    default void applyReadableMetadata(List<String> metadataKeys) {
        throw new UnsupportedOperationException(
                "A decoding format must override this method to apply metadata keys.");
    }
}
