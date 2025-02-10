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

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.runtime.operators.process.ProcessTableOperator;
import org.apache.flink.util.Collector;

/**
 * Abstraction of code-generated calls to {@link ProcessTableFunction} to be used within {@link
 * ProcessTableOperator}.
 */
public abstract class ProcessTableRunner extends AbstractRichFunction {

    public Collector<RowData> runnerCollector;
    public ProcessTableFunction.Context runnerContext;

    /**
     * @param stateToFunction state entries to be converted into external data structure; null if
     *     state is empty
     * @param stateCleared reference to whether the state has been cleared within the function; if
     *     yes, a conversion from external to internal data structure is not necessary anymore
     * @param stateFromFunction state ready for persistence; null if {@param stateCleared} was true
     *     during conversion
     * @param inputIndex index of the currently processed input table
     * @param row data of the currently processed input table
     */
    public abstract void processElement(
            RowData[] stateToFunction,
            boolean[] stateCleared,
            RowData[] stateFromFunction,
            int inputIndex,
            RowData row)
            throws Exception;
}
