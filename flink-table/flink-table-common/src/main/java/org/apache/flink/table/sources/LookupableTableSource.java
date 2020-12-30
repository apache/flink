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

package org.apache.flink.table.sources;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;

/**
 * A {@link TableSource} which supports for lookup accessing via key column(s). For example, MySQL
 * TableSource can implement this interface to support lookup accessing. When temporal join this
 * MySQL table, the runtime behavior could be in a lookup fashion.
 *
 * @param <T> type of the result
 * @deprecated This interface will not be supported in the new source design around {@link
 *     DynamicTableSource} which only works with the Blink planner. Use {@link LookupTableSource}
 *     instead. See FLIP-95 for more information.
 */
@Deprecated
@Experimental
public interface LookupableTableSource<T> extends TableSource<T> {

    /**
     * Gets the {@link TableFunction} which supports lookup one key at a time.
     *
     * @param lookupKeys the chosen field names as lookup keys, it is in the defined order
     */
    TableFunction<T> getLookupFunction(String[] lookupKeys);

    /**
     * Gets the {@link AsyncTableFunction} which supports async lookup one key at a time.
     *
     * @param lookupKeys the chosen field names as lookup keys, it is in the defined order
     */
    AsyncTableFunction<T> getAsyncLookupFunction(String[] lookupKeys);

    /**
     * Returns true if async lookup is enabled.
     *
     * <p>The lookup function returned by {@link #getAsyncLookupFunction(String[])} will be used if
     * returns true. Otherwise, the lookup function returned by {@link #getLookupFunction(String[])}
     * will be used.
     */
    boolean isAsyncEnabled();
}
