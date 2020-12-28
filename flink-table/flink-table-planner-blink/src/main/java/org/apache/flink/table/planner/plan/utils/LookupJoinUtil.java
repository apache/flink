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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rex.RexLiteral;

/** Utilities for lookup joins using {@link LookupTableSource}. */
@Internal
public final class LookupJoinUtil {

    /** A field used as an equal condition when querying content from a dimension table. */
    public static class LookupKey {
        private LookupKey() {
            // sealed class
        }
    }

    /** A {@link LookupKey} whose value is constant. */
    public static class ConstantLookupKey extends LookupKey {
        public final LogicalType sourceType;
        public final RexLiteral literal;

        public ConstantLookupKey(LogicalType sourceType, RexLiteral literal) {
            this.sourceType = sourceType;
            this.literal = literal;
        }
    }

    /** A {@link LookupKey} whose value comes from the left table field. */
    public static class FieldRefLookupKey extends LookupKey {
        public final int index;

        public FieldRefLookupKey(int index) {
            this.index = index;
        }
    }

    private LookupJoinUtil() {
        // no instantiation
    }
}
