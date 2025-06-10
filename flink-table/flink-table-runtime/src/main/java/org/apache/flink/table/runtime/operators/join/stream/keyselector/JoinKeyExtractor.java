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

package org.apache.flink.table.runtime.operators.join.stream.keyselector;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Defines the strategy for extracting keys used for state management within the {@link
 * StreamingMultiJoinOperator}.
 *
 * <p>Implementations define how to derive keys for storing state records and for looking up
 * matching records in state during the join process.
 */
public interface JoinKeyExtractor extends Serializable {

    /**
     * Extracts the key used for storing the input record in its corresponding state view.
     *
     * <p>This key determines how records from a specific input stream are organized within their
     * state.
     *
     * @param row The input row for which to extract the storage key.
     * @param inputId The ID (0-based index) of the input stream this row belongs to.
     * @return A {@link RowData} representing the state storage key. Can be null if no key can be
     *     derived (e.g., missing configuration).
     */
    RowData getJoinKey(RowData row, int inputId);

    /**
     * Extracts the key used for looking up matching records in the state of a specific input depth,
     * based on the rows accumulated from previous inputs.
     *
     * <p>When processing the join recursively at a certain `depth`, this key is used to query the
     * state associated with that `depth` (i.e., the state for input `depth`) to find potential join
     * partners.
     *
     * @param depth The current processing depth (0-based index), representing the target input ID
     *     for state lookup.
     * @param joinedRowData JoinedRowData with rows accumulated so far in the current recursive join
     *     path.
     * @return A {@link RowData} representing the state lookup key. Can be null if no key can be
     *     derived (e.g., missing configuration, or a required row in `joinedRowData` is null).
     */
    RowData getLeftSideJoinKey(int depth, RowData joinedRowData);

    /**
     * Returns the type of the join key for a given input.
     *
     * @param inputId The ID of the input stream.
     * @return The {@link RowType} of the join key.
     */
    @Nullable
    RowType getJoinKeyType(int inputId);

    /**
     * Gets the field indices in the source row that make up the join key for a given input.
     *
     * @param inputId The ID of the input stream.
     * @return An array of integers representing the field indices in the source row that form the
     *     join key.
     */
    int[] getJoinKeyIndices(int inputId);

    /**
     * Extracts the common key from an input row. The common key consists of attributes that are
     * part of all equi-join conditions in the multi-join sequence.
     *
     * @param row The input row.
     * @param inputId The ID of the input stream this row belongs to.
     * @return A {@link RowData} representing the common key, or a default key if no common
     *     attributes exist or cannot be determined for this input.
     */
    RowData getCommonJoinKey(RowData row, int inputId);

    /**
     * Gets the type information for the common join key.
     *
     * @return The {@link RowType} for the common key join type.
     */
    RowType getCommonJoinKeyType();

    /**
     * Gets the field indices in the source row that make up the common join key for a given input.
     *
     * @param inputId The ID of the input stream.
     * @return An array of integers representing the field indices in the source row that form the
     *     common join key.
     */
    int[] getCommonJoinKeyIndices(int inputId);
}
