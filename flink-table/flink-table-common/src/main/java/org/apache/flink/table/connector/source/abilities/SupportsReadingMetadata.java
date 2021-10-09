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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Interface for {@link ScanTableSource}s that support reading metadata columns.
 *
 * <p>Metadata columns add additional columns to the table's schema. A table source is responsible
 * for adding requested metadata columns at the end of produced rows. This includes potentially
 * forwarding metadata columns from contained formats.
 *
 * <p>Examples in SQL look like:
 *
 * <pre>{@code
 * // reads the column from corresponding metadata key `timestamp`
 * CREATE TABLE t1 (i INT, s STRING, timestamp TIMESTAMP(3) WITH LOCAL TIME ZONE METADATA, d DOUBLE)
 *
 * // reads the column from metadata key `timestamp` and casts to INT
 * CREATE TABLE t2 (i INT, s STRING, myTimestamp INT METADATA FROM 'timestamp', d DOUBLE)
 * }</pre>
 *
 * <p>By default, if this interface is not implemented, the statements above would fail because the
 * table source does not provide a metadata key called `timestamp`.
 *
 * <p>If this interface is implemented, {@link #listReadableMetadata()} lists all metadata keys and
 * their corresponding data types that the source exposes to the planner. The planner will use this
 * information for validation and insertion of explicit casts if necessary.
 *
 * <p>The planner will select required metadata columns (i.e. perform projection push down) and will
 * call {@link #applyReadableMetadata(List, DataType)} with a list of metadata keys. An
 * implementation must ensure that metadata columns are appended at the end of the physical row in
 * the order of the provided list after the apply method has been called, e.g. using {@link
 * JoinedRowData}.
 *
 * <p>Note: The final output data type emitted by a source changes from the physically produced data
 * type to a data type with metadata columns. {@link #applyReadableMetadata(List, DataType)} will
 * pass the updated data type for convenience. If a source implements {@link
 * SupportsProjectionPushDown}, the projection must be applied to the physical data in the first
 * step. The passed updated data type will have considered information from {@link
 * SupportsProjectionPushDown} already.
 *
 * <p>The metadata column's data type must match with {@link #listReadableMetadata()}. For the
 * examples above, this means that a table source for `t2` returns a TIMESTAMP and not INT. The
 * casting to INT will be performed by the planner in a subsequent operation:
 *
 * <pre>{@code
 * // for t1 and t2
 * ROW < i INT, s STRING, d DOUBLE >                                              // physical output
 * ROW < i INT, s STRING, d DOUBLE, timestamp TIMESTAMP(3) WITH LOCAL TIME ZONE > // final output
 * }</pre>
 */
@PublicEvolving
public interface SupportsReadingMetadata {

    /**
     * Returns the map of metadata keys and their corresponding data types that can be produced by
     * this table source for reading.
     *
     * <p>The returned map will be used by the planner for validation and insertion of explicit
     * casts (see {@link LogicalTypeCasts#supportsExplicitCast(LogicalType, LogicalType)}) if
     * necessary.
     *
     * <p>The iteration order of the returned map determines the order of metadata keys in the list
     * passed in {@link #applyReadableMetadata(List, DataType)}. Therefore, it might be beneficial
     * to return a {@link LinkedHashMap} if a strict metadata column order is required.
     *
     * <p>If a source forwards metadata from one or more formats, we recommend the following column
     * order for consistency:
     *
     * <pre>{@code
     * KEY FORMAT METADATA COLUMNS + VALUE FORMAT METADATA COLUMNS + SOURCE METADATA COLUMNS
     * }</pre>
     *
     * <p>Metadata key names follow the same pattern as mentioned in {@link Factory}. In case of
     * duplicate names in format and source keys, format keys shall have higher precedence.
     *
     * <p>Regardless of the returned {@link DataType}s, a metadata column is always represented
     * using internal data structures (see {@link RowData}).
     *
     * @see DecodingFormat#listReadableMetadata()
     */
    Map<String, DataType> listReadableMetadata();

    /**
     * Provides a list of metadata keys that the produced {@link RowData} must contain as appended
     * metadata columns.
     *
     * <p>Implementations of this method must be idempotent. The planner might call this method
     * multiple times.
     *
     * <p>Note: Use the passed data type instead of {@link ResolvedSchema#toPhysicalRowDataType()}
     * for describing the final output data type when creating {@link TypeInformation}. If the
     * source implements {@link SupportsProjectionPushDown}, the projection is already considered in
     * the given output data type.
     *
     * @param metadataKeys a subset of the keys returned by {@link #listReadableMetadata()}, ordered
     *     by the iteration order of returned map
     * @param producedDataType the final output type of the source
     * @see DecodingFormat#applyReadableMetadata(List)
     */
    void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType);

    /**
     * Defines whether projections can be applied to metadata columns.
     *
     * <p>This method is only called if the source does <em>not</em> implement {@link
     * SupportsProjectionPushDown}. By default, the planner will only apply metadata columns which
     * have actually been selected in the query regardless. By returning {@code false} instead the
     * source can inform the planner to apply all metadata columns defined in the table's schema.
     *
     * <p>If the source implements {@link SupportsProjectionPushDown}, projections of metadata
     * columns are always considered before calling {@link #applyReadableMetadata(List, DataType)}.
     */
    default boolean supportsMetadataProjection() {
        return true;
    }
}
