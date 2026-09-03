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

package org.apache.flink.table.runtime.operators.sink.constraint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER;

/** Enforces NOT NULL constraints on the input {@link RowData}. */
@Internal
final class NotNullConstraint implements Constraint {
    private final NotNullEnforcementStrategy enforcementStrategy;
    private final int[] notNullFieldIndices;
    private final String[] notNullFieldNames;
    // Whether the input is a key-only-delete pipeline, in which a DELETE tombstone carries only its
    // key and sets all non-key fields to null regardless of nullability constraints.
    private final boolean keyOnlyDeletes;
    // Aligned with notNullFieldIndices: true if the field is part of the primary key.
    private final boolean[] keyField;

    NotNullConstraint(
            NotNullEnforcementStrategy enforcementStrategy,
            int[] notNullFieldIndices,
            String[] notNullFieldNames,
            boolean keyOnlyDeletes,
            boolean[] keyField) {
        this.enforcementStrategy = enforcementStrategy;
        this.notNullFieldIndices = notNullFieldIndices;
        this.notNullFieldNames = notNullFieldNames;
        this.keyOnlyDeletes = keyOnlyDeletes;
        this.keyField = keyField;
    }

    @Nullable
    @Override
    public RowData enforce(RowData input) {
        // A key-only DELETE legitimately carries null in its non-key columns, so skip those.
        // Key columns and all columns of other row kinds are always enforced.
        final boolean partialDelete = keyOnlyDeletes && input.getRowKind() == RowKind.DELETE;
        for (int i = 0; i < notNullFieldIndices.length; i++) {
            if (partialDelete && !keyField[i]) {
                continue;
            }
            final int index = notNullFieldIndices[i];
            if (input.isNullAt(index)) {
                switch (enforcementStrategy) {
                    case ERROR:
                        throw new EnforcerException(
                                "Column '%s' is NOT NULL, however, a null value is being written into it. "
                                        + String.format(
                                                "You can set job configuration '%s'='%s' "
                                                        + "to suppress this exception and drop such records silently.",
                                                TABLE_EXEC_SINK_NOT_NULL_ENFORCER.key(),
                                                ExecutionConfigOptions.NotNullEnforcer.DROP.name()),
                                notNullFieldNames[i]);
                    case DROP:
                        return null;
                }
            }
        }
        return input;
    }

    @Override
    public String toString() {
        return String.format("NotNullEnforcer(fields=[%s])", String.join(", ", notNullFieldNames));
    }
}
