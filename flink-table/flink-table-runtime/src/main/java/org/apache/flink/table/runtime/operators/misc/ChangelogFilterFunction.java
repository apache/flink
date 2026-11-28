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

package org.apache.flink.table.runtime.operators.misc;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.util.EnumSet;

/**
 * A function that keeps only the rows whose {@link RowKind} is in a given set. The planner uses it
 * to drop {@link RowKind#UPDATE_BEFORE} and {@link RowKind#DELETE} rows in front of operators that
 * ignore retractions.
 */
public class ChangelogFilterFunction implements FilterFunction<RowData> {
    private static final long serialVersionUID = 1L;

    private final EnumSet<RowKind> keptRowKinds;

    public ChangelogFilterFunction(EnumSet<RowKind> keptRowKinds) {
        this.keptRowKinds = EnumSet.copyOf(keptRowKinds);
    }

    @Override
    public boolean filter(RowData value) {
        return keptRowKinds.contains(value.getRowKind());
    }
}
