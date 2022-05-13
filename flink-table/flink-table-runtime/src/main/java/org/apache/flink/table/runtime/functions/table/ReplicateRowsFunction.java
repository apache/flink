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

package org.apache.flink.table.runtime.functions.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;

/**
 * Replicate the row N times. N is specified as the first argument to the function. This is an
 * internal function solely used by optimizer to rewrite EXCEPT ALL AND INTERSECT ALL queries.
 */
@Internal
public class ReplicateRowsFunction extends BuiltInTableFunction<RowData> {

    private static final long serialVersionUID = 1L;

    public ReplicateRowsFunction(SpecializedContext specializedContext) {
        super(BuiltInFunctionDefinitions.INTERNAL_REPLICATE_ROWS, specializedContext);
    }

    public void eval(Object... inputs) {
        final long replication = (long) inputs[0];
        final int rowLength = inputs.length - 1;

        final GenericRowData row = new GenericRowData(rowLength);
        for (int i = 0; i < rowLength; i++) {
            row.setField(i, inputs[i + 1]);
        }

        for (int i = 0; i < replication; i++) {
            collect(row);
        }
    }
}
