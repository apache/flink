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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.utils.EncodingUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Immutable columns constraint is used to identify which columns in a table are not allowed to be
 * modified.
 *
 * @see ConstraintType
 */
@PublicEvolving
public final class ImmutableColumnsConstraint extends AbstractConstraint {

    private final List<String> columns;
    private final ConstraintType type;

    /** Creates a non enforced {@link ConstraintType#IMMUTABLE_COLUMNS} constraint. */
    public static ImmutableColumnsConstraint immutableColumns(String name, List<String> columns) {
        return new ImmutableColumnsConstraint(
                name, false, ConstraintType.IMMUTABLE_COLUMNS, columns);
    }

    private ImmutableColumnsConstraint(
            String name, boolean enforced, ConstraintType type, List<String> columns) {
        super(name, enforced);

        this.columns = columns;
        this.type = type;

        if (type != ConstraintType.IMMUTABLE_COLUMNS) {
            throw new IllegalStateException("Unknown key type: " + getType());
        }
    }

    public List<String> getColumns() {
        return columns;
    }

    @Override
    public ConstraintType getType() {
        return ConstraintType.IMMUTABLE_COLUMNS;
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "CONSTRAINT %s COLUMNS (%s) IMMUTABLE%s",
                EncodingUtils.escapeIdentifier(getName()),
                columns.stream()
                        .map(EncodingUtils::escapeIdentifier)
                        .collect(Collectors.joining(", ")),
                isEnforced() ? "" : " NOT ENFORCED");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ImmutableColumnsConstraint that = (ImmutableColumnsConstraint) o;
        return Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), columns);
    }
}
