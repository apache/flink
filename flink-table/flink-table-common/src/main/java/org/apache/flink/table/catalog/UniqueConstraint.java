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

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A unique key constraint. It can be declared also as a PRIMARY KEY.
 *
 * @see ConstraintType
 */
@PublicEvolving
public final class UniqueConstraint extends AbstractConstraint {

    private final List<String> columns;
    private final ConstraintType type;

    /** Creates a non enforced {@link ConstraintType#PRIMARY_KEY} constraint. */
    public static UniqueConstraint primaryKey(String name, List<String> columns) {
        return new UniqueConstraint(name, false, ConstraintType.PRIMARY_KEY, columns);
    }

    private UniqueConstraint(
            String name, boolean enforced, ConstraintType type, List<String> columns) {
        super(name, enforced);

        this.columns = checkNotNull(columns);
        this.type = checkNotNull(type);
    }

    /** List of column names for which the primary key was defined. */
    public List<String> getColumns() {
        return columns;
    }

    @Override
    public ConstraintType getType() {
        return type;
    }

    /**
     * Returns constraint's summary. All constraints summary will be formatted as
     *
     * <pre>
     * CONSTRAINT [constraint-name] [constraint-type] ([constraint-definition])
     *
     * E.g CONSTRAINT pk PRIMARY KEY (f0, f1) NOT ENFORCED
     * </pre>
     */
    @Override
    public final String asSummaryString() {
        final String typeString;
        switch (getType()) {
            case PRIMARY_KEY:
                typeString = "PRIMARY KEY";
                break;
            case UNIQUE_KEY:
                typeString = "UNIQUE";
                break;
            default:
                throw new IllegalStateException("Unknown key type: " + getType());
        }

        return String.format(
                "CONSTRAINT %s %s (%s)%s",
                EncodingUtils.escapeIdentifier(getName()),
                typeString,
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
        UniqueConstraint that = (UniqueConstraint) o;
        return Objects.equals(columns, that.columns) && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), columns, type);
    }
}
