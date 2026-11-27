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

package org.apache.flink.table.runtime.functions.ptf;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;

/**
 * {@link Deduplicate} on updating input whose PARTITION BY columns contain an upsert key: a key has
 * at most one live row, so {@code keep} and {@code sequence_by} have no effect and every change is
 * forwarded without state. A -U becomes a -D if the output has no -U, since the row may have moved
 * to another key.
 */
@Internal
public class DeduplicatePassThroughFunction extends DeduplicateUpdatingFunctionBase {

    private static final long serialVersionUID = 1L;

    DeduplicatePassThroughFunction(final SpecializedContext context) {
        super(context, new LinkedHashMap<>());
    }

    public void eval(
            final Context ctx,
            final RowData input,
            @Nullable final StringData keep,
            @Nullable final ColumnList sequenceBy,
            @Nullable final Long stateTtl) {
        final RowKind kind = input.getRowKind();
        if (kind == RowKind.UPDATE_BEFORE && !isUpdateBeforeEmitted) {
            emit(RowKind.DELETE, input);
        } else {
            emit(kind, input);
        }
    }
}
