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

package org.apache.flink.table.connector.sink.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.sink.DynamicTableSink;

/**
 * Interface for {@link DynamicTableSink}s that support target column writing.
 *
 * <p>The planner will parse target columns from the DML clause and call {@link
 * #applyTargetColumns(int[][])} to pass an array of column index paths to the sink.
 *
 * <p>The array indices are 0-based and support composite columns within (possibly nested)
 * structures. This information comes from the column list of the DML clause, e.g., for a sink table
 * t1 whose schema is: {@code a STRING, b ROW < b1 INT, b2 STRING>, c BIGINT}
 *
 * <ul>
 *   <li>insert: 'insert into t1(a, b.b2) ...', the column list will be 'a, b.b2', and will provide
 *       {@code [[0], [1, 1]]}. The statement 'insert into t1 select ...' will provide an empty list
 *       and will not apply this ability.
 *   <li>update: 'update t1 set a=1, b.b1=2 where ...', the column list will be 'a, b.b1', and will
 *       provide {@code [[0], [1, 0]]}.
 * </ul>
 *
 * <p>Note: Planner will not apply this ability for the delete statement because it has no column
 * list.
 *
 * <p>A sink can use this information to perform target columns writing.
 *
 * <p>If this interface is implemented and {@link #applyTargetColumns(int[][])} returns true. The
 * planner will use this information for plan optimization such as sink reuse.
 */
@PublicEvolving
public interface SupportsTargetColumnWriting {

    /**
     * Provides an array of column index paths related to user specified target column list.
     *
     * <p>See the documentation of {@link SupportsTargetColumnWriting} for more information.
     *
     * @param targetColumns column index paths
     * @return true if the target columns are applied successfully, false otherwise.
     */
    boolean applyTargetColumns(int[][] targetColumns);
}
