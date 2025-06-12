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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.utils.EncodingUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents a basic implementation of an {@link Index} for a table.
 *
 * <p>It serves as a foundational index type without any uniqueness constraints.
 */
@Internal
public class DefaultIndex implements Index {

    private final String name;

    private final List<String> columns;

    public static DefaultIndex newIndex(String name, List<String> columns) {
        return new DefaultIndex(name, columns);
    }

    private DefaultIndex(String name, List<String> columns) {
        this.name = name;
        this.columns = columns;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<String> getColumns() {
        return columns;
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "INDEX %s (%s)",
                EncodingUtils.escapeIdentifier(getName()),
                columns.stream()
                        .map(EncodingUtils::escapeIdentifier)
                        .collect(Collectors.joining(", ")));
    }

    @Override
    public String toString() {
        return asSummaryString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultIndex index = (DefaultIndex) o;
        return Objects.equals(name, index.name) && Objects.equals(columns, index.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, columns);
    }
}
