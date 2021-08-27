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

package org.apache.flink.table.sources;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;

/**
 * Adds support for projection push-down to a {@link TableSource} with nested fields.
 *
 * <p>A {@link TableSource} extending this interface is able to project the fields of its returned
 * {@code DataStream} if it is a {@code StreamTableSource}.
 *
 * @param <T> The return type of the {@link TableSource}.
 * @deprecated This interface will not be supported in the new source design around {@link
 *     DynamicTableSource}. Use {@link SupportsProjectionPushDown} instead. See FLIP-95 for more
 *     information.
 */
@Deprecated
@PublicEvolving
public interface NestedFieldsProjectableTableSource<T> {

    /**
     * Creates a copy of the {@link TableSource} that projects its output to the given field
     * indexes. The field indexes relate to the physical produced data type ({@link
     * TableSource#getProducedDataType()}) and not to the table schema ({@link
     * TableSource#getTableSchema()} of the {@link TableSource}.
     *
     * <p>The table schema ({@link TableSource#getTableSchema()} of the {@link TableSource} copy
     * must not be modified by this method, but only the produced data type ({@link
     * TableSource#getProducedDataType()}) and the produced {@code DataStream} ({@code
     * StreamTableSource.getDataStream()}). The produced data type may only be changed by removing
     * or reordering first level fields. The type of the first level fields must not be changed.
     *
     * <p>If the {@link TableSource} implements the {@link DefinedFieldMapping} interface, it might
     * be necessary to adjust the mapping as well.
     *
     * <p>The {@code nestedFields} parameter contains all nested fields that are accessed by the
     * query. This information can be used to only read and set the accessed fields. Non-accessed
     * fields may be left empty, set to null, or to a default value.
     *
     * <p>This method is called with parameters as shown in the example below:
     *
     * <pre>{@code
     * // schema
     * tableSchema = {
     *     id,
     *     student<\school<\city, tuition>, age, name>,
     *     teacher<\age, name>
     * }
     *
     * // query
     * select (id, student.school.city, student.age, teacher)
     *
     * // parameters
     * fields = field = [0, 1, 2]
     * nestedFields  \[\["*"], ["school.city", "age"], ["*"\]\]
     * }</pre>
     *
     * <p>IMPORTANT: This method must return a true copy and must not modify the original table
     * source object.
     *
     * @param fields The indexes of the fields to return.
     * @param nestedFields The paths of all nested fields which are accessed by the query. All other
     *     nested fields may be empty.
     * @return A copy of the {@link TableSource} that projects its output.
     */
    TableSource<T> projectNestedFields(int[] fields, String[][] nestedFields);
}
