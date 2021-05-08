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
import org.apache.flink.util.Collector;

/** The base class for handling table aggregate functions with namespace. */
public interface NamespaceTableAggsHandleFunction<N> extends NamespaceAggsHandleFunctionBase<N> {

    /**
     * Emits the result of the aggregation from the current accumulators and namespace properties
     * (like window start).
     *
     * @param namespace the namespace properties which should be calculated, such window start
     * @param key the group key for the current emit.
     * @param out the collector used to emit results.
     */
    void emitValue(N namespace, RowData key, Collector<RowData> out) throws Exception;
}
