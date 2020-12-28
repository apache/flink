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
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;

/**
 * Adds support for limiting push-down to a {@link TableSource}. A {@link TableSource} extending
 * this interface is able to limit the number of records.
 *
 * <p>After pushing down, source only needs to try its best to limit the number of output records,
 * but does not need to guarantee that the number must be less than or equal to the limit.
 *
 * @deprecated This interface will not be supported in the new source design around {@link
 *     DynamicTableSource} which only works with the Blink planner. Use {@link
 *     SupportsLimitPushDown} instead. See FLIP-95 for more information.
 */
@Deprecated
@Experimental
public interface LimitableTableSource<T> {

    /**
     * Return the flag to indicate whether limit push down has been tried. Must return true on the
     * returned instance of {@link #applyLimit(long)}.
     */
    boolean isLimitPushedDown();

    /**
     * Check and push down the limit to the table source.
     *
     * @param limit the value which limit the number of records.
     * @return A new cloned instance of {@link TableSource}.
     */
    TableSource<T> applyLimit(long limit);
}
