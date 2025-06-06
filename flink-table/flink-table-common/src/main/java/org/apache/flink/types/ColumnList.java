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

package org.apache.flink.types;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Represents a list of columns.
 *
 * <p>This type is the return type of calls to {@code DESCRIPTOR(`c0`, `c1`)}. The type is intended
 * to be used in arguments of {@link ProcessTableFunction}s.
 *
 * @see DataTypes#DESCRIPTOR()
 */
@PublicEvolving
public final class ColumnList implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<String> names;
    private final List<DataType> dataTypes;

    private ColumnList(List<String> names, List<DataType> dataTypes) {
        this.names = Preconditions.checkNotNull(names, "Names must not be null.");
        this.dataTypes = Preconditions.checkNotNull(dataTypes, "Data types must be null.");
        Preconditions.checkArgument(
                dataTypes.isEmpty() || dataTypes.size() == names.size(),
                "Mismatch between data types and names.");
    }

    public static ColumnList of(List<String> names, List<DataType> dataTypes) {
        return new ColumnList(List.copyOf(names), List.copyOf(dataTypes));
    }

    public static ColumnList of(List<String> names) {
        return of(names, List.of());
    }

    /**
     * Returns a list of column names.
     *
     * <p>For example, it returns [a, b, c] for function calls like {@code DESCRIPTOR(a INT, b
     * STRING, c BOOLEAN)} or {@code DESCRIPTOR(a, b, c)}.
     */
    public List<String> getNames() {
        return names;
    }

    /**
     * Returns a list of data types (if available).
     *
     * <p>For example, it returns [INT, STRING, BOOLEAN] for function calls like {@code DESCRIPTOR(a
     * INT, b STRING, c BOOLEAN)}.
     *
     * <p>Note: The list might be empty if only names were passed to the descriptor function like
     * {@code DESCRIPTOR(a, b, c)}. Thus, the list is either empty or has the same number of
     * elements as {@link #getNames()}.
     */
    public List<DataType> getDataTypes() {
        return dataTypes;
    }

    @Override
    public String toString() {
        return IntStream.range(0, names.size())
                .mapToObj(
                        pos -> {
                            final String name = EncodingUtils.escapeIdentifier(names.get(pos));
                            if (dataTypes.isEmpty()) {
                                return name;
                            } else {
                                final String dataType =
                                        dataTypes.get(pos).getLogicalType().asSummaryString();
                                return name + " " + dataType;
                            }
                        })
                .collect(Collectors.joining(", ", "(", ")"));
    }
}
