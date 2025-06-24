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

import java.io.IOException;
import java.util.Collection;

/**
 * A wrapper class of {@link TableFunction} for synchronously lookup rows matching the lookup keys
 * from external system.
 *
 * <p>The output type of this table function is fixed as {@link RowData}.
 */
@PublicEvolving
public abstract class LookupFunction extends TableFunction<RowData> {

    /**
     * Synchronously lookup rows matching the lookup keys.
     *
     * <p>Please note that the returning collection of RowData shouldn't be reused across
     * invocations.
     *
     * @param keyRow - A {@link RowData} that wraps lookup keys.
     * @return A collection of all matching rows in the lookup table.
     */
    public abstract Collection<RowData> lookup(RowData keyRow) throws IOException;

    /** Invoke {@link #lookup} and handle exceptions. */
    public final void eval(Object... keys) {
        GenericRowData keyRow = GenericRowData.of(keys);
        try {
            Collection<RowData> lookup = lookup(keyRow);
            if (lookup == null) {
                return;
            }
            lookup.forEach(this::collect);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Failed to lookup values with given key row '%s'", keyRow), e);
        }
    }
}
