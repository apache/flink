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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Collection;

/**
 * A wrapper class of {@link TableFunction} for synchronous model inference.
 *
 * <p>The output type of this table function is fixed as {@link RowData}.
 */
@PublicEvolving
public abstract class PredictFunction extends TableFunction<RowData> {

    /**
     * Synchronously predict result based on input row.
     *
     * @param inputRow - A {@link RowData} that wraps input for predict function.
     * @return A collection of predicted results.
     */
    public abstract Collection<RowData> predict(RowData inputRow);

    /** Invoke {@link #predict} and handle exceptions. */
    public final void eval(Object... args) {
        GenericRowData argsData = GenericRowData.of(args);
        try {
            Collection<RowData> results = predict(argsData);
            if (results == null) {
                return;
            }
            results.forEach(this::collect);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to execute prediction with input row %s.", argsData), e);
        }
    }
}
