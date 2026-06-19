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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * The base class for handling aggregate functions.
 *
 * <p>It is code generated to handle all {@link AggregateFunction}s together in an aggregation.
 *
 * <p>It is the entry point for aggregate operators to operate all {@link AggregateFunction}s.
 */
public interface AggsHandleFunction extends AggsHandleFunctionBase {

    /**
     * Gets the result of the aggregation from the current accumulators.
     *
     * @return the final result (saved in a row) of the current accumulators.
     */
    RowData getValue() throws Exception;

    /**
     * Set window size for the aggregate function. It's used in batch scenario to set the total rows
     * of the current window frame. More information for window frame:
     * https://docs.oracle.com/cd/E17952_01/mysql-8.0-en/window-functions-frames.html
     *
     * <p>We may need to set the value for some window functions may require the total rows of
     * current window frame to do calculation. For example, the function PERCENT_RANK need to know
     * the window's size, and the SQL looks like: <code>
     * SELECT PERCENT_RANK() OVER ([ partition_by_clause] order_by_clause)</code>.
     */
    void setWindowSize(int windowSize);
}
