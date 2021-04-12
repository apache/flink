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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.ScanTableSource;

/**
 * Enables to push down a (possibly nested) projection into a {@link ScanTableSource}.
 *
 * <p>Given the following SQL:
 *
 * <pre>{@code
 * CREATE TABLE t (i INT, r ROW < d DOUBLE, b BOOLEAN>, s STRING);
 * SELECT s, r.d FROM t;
 * }</pre>
 *
 * <p>In the above example, {@code r.d} and {@code s} are required fields. Other fields can be
 * skipped in a projection. Compared to table's schema, fields are reordered.
 *
 * <p>By default, if this interface is not implemented, a projection is applied in a subsequent
 * operation after the source.
 *
 * <p>For efficiency, a source can push a projection further down in order to be close to the actual
 * data generation. A projection is only selecting fields that are used by a query (possibly in a
 * different field order). It does not contain any computation. A projection can either be performed
 * on the fields of the top-level row only or consider nested fields as well (see {@link
 * #supportsNestedProjection()}).
 */
@PublicEvolving
public interface SupportsProjectionPushDown {

    /** Returns whether this source supports nested projection. */
    boolean supportsNestedProjection();

    /**
     * Provides the field index paths that should be used for a projection. The indices are 0-based
     * and support fields within (possibly nested) structures if this is enabled via {@link
     * #supportsNestedProjection()}.
     *
     * <p>In the example mentioned in {@link SupportsProjectionPushDown}, this method would receive:
     *
     * <ul>
     *   <li>{@code [[2], [1]]} which is equivalent to {@code [["s"], ["r"]]} if {@link
     *       #supportsNestedProjection()} returns false.
     *   <li>{@code [[2], [1, 0]]} which is equivalent to {@code [["s"], ["r", "d"]]]} if {@link
     *       #supportsNestedProjection()} returns true.
     * </ul>
     *
     * @param projectedFields field index paths of all fields that must be present in the physically
     *     produced data
     */
    void applyProjection(int[][] projectedFields);
}
