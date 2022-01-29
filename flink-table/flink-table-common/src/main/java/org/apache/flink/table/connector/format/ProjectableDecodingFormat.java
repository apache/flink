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
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.types.DataType;

/**
 * Extension of {@link DecodingFormat} which is able to produce projected rows.
 *
 * <p>For more details on usage and differences between {@link DecodingFormat} and {@link
 * ProjectableDecodingFormat}, check the documentation of {@link DecodingFormat}.
 *
 * @see Projection
 * @see ProjectedRowData
 */
@PublicEvolving
public interface ProjectableDecodingFormat<I> extends DecodingFormat<I> {

    /** Returns whether this format supports nested projection. */
    default boolean supportsNestedProjection() {
        return false;
    }

    /**
     * Creates runtime decoder implementation that is configured to produce data of type {@code
     * Projection.of(projections).project(physicalDataType)}. For more details on the usage, check
     * {@link DecodingFormat} documentation.
     *
     * @param context the context provides several utilities required to instantiate the runtime
     *     decoder implementation of the format
     * @param physicalDataType For more details check {@link DecodingFormat}
     * @param projections the projections array. The array represents the mapping of the fields of
     *     the original {@link DataType}, including nested rows. For example, {@code [[0, 2, 1],
     *     ...]} specifies to include the 2nd field of the 3rd field of the 1st field in the
     *     top-level row. It's guaranteed that this array won't contain nested projections if {@link
     *     #supportsNestedProjection()} returns {@code false}. For more details, check {@link
     *     Projection} as well.
     * @return the runtime decoder
     * @see DecodingFormat
     */
    I createRuntimeDecoder(
            DynamicTableSource.Context context, DataType physicalDataType, int[][] projections);

    default I createRuntimeDecoder(
            DynamicTableSource.Context context, DataType projectedPhysicalDataType) {
        return createRuntimeDecoder(
                context,
                projectedPhysicalDataType,
                Projection.all(projectedPhysicalDataType).toNestedIndexes());
    }
}
