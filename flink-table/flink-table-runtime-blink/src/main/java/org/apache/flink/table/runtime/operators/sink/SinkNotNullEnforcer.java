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

package org.apache.flink.table.runtime.operators.sink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer;
import org.apache.flink.table.data.RowData;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Checks writing null values into NOT NULL columns. */
public class SinkNotNullEnforcer implements FilterFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private final NotNullEnforcer notNullEnforcer;
    private final int[] notNullFieldIndices;
    private final String[] allFieldNames;

    public SinkNotNullEnforcer(
            NotNullEnforcer notNullEnforcer, int[] notNullFieldIndices, String[] allFieldNames) {
        checkArgument(
                notNullFieldIndices.length > 0,
                "SinkNotNullEnforcer requires that there are not-null fields.");
        this.notNullFieldIndices = notNullFieldIndices;
        this.notNullEnforcer = notNullEnforcer;
        this.allFieldNames = allFieldNames;
    }

    @Override
    public boolean filter(RowData row) {
        for (int index : notNullFieldIndices) {
            if (row.isNullAt(index)) {
                if (notNullEnforcer == NotNullEnforcer.ERROR) {
                    String optionKey =
                            ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER.key();
                    throw new TableException(
                            String.format(
                                    "Column '%s' is NOT NULL, however, a null value is being written into it. "
                                            + "You can set job configuration '"
                                            + optionKey
                                            + "'='drop' "
                                            + "to suppress this exception and drop such records silently.",
                                    allFieldNames[index]));
                } else {
                    // simply drop the record
                    return false;
                }
            }
        }
        return true;
    }
}
