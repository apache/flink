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

package org.apache.flink.table.runtime.operators.join.stream.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;

public interface OuterMultiJoinStateView extends MultiJoinStateView {
    /**
     * Adds a new record with the number of associations to the state view.
     *
     * @param joinKey joinKey of a record
     * @param record the added record
     * @param numOfAssociations the number of records associated with another side
     */
    void addRecord(RowData joinKey, RowData record, int numOfAssociations) throws Exception;

    /**
     * Adds a new record with record count and the number of associations to the state view. This
     * method is needed mainly in case InputSideHasNoUniqueKey to avoid unnecessary read operation
     * from the state.
     *
     * @param joinKey joinKey of a record
     * @param record the added record
     * @param value the tuple of record count and number of associations
     */
    void addRecord(RowData joinKey, RowData record, Tuple2<Integer, Integer> value)
            throws Exception;

    /**
     * Updates the number of associations belongs to the record.
     *
     * @param joinKey joinKey of a record
     * @param record the record to update
     * @param numOfAssociations the new number of records associated with other side
     */
    void updateNumOfAssociations(RowData joinKey, RowData record, int numOfAssociations)
            throws Exception;

    /**
     * Gets all the records and number of associations under the current join key.
     *
     * @param joinKey joinKey of a record
     */
    Iterable<Tuple2<RowData, Integer>> getRecordsAndNumOfAssociations(RowData joinKey)
            throws Exception;

    /**
     * Gets all the records with its count and number of associations under the current join key.
     * This method is needed mainly in case InputSideHasNoUniqueKey to avoid unnecessary read
     * operations from the state.
     *
     * @param joinKey joinKey of a record
     */
    Iterable<Tuple2<RowData, Tuple2<Integer, Integer>>> getRecordsCountAndNumOfAssociations(
            RowData joinKey) throws Exception;
}
