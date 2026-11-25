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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.GeneratedComparator;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.functions.TableSemantics.SortDirection;
import org.apache.flink.table.functions.definitions.DeduplicateDefinition.Keep;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.utils.CallContextUtils;

import java.io.Serializable;

import static org.apache.flink.table.functions.definitions.DeduplicateDefinition.ARG_INPUT;

/** Record ordering of the built-in deduplication PTFs. */
@Internal
public final class DeduplicateUtils {

    public static TableSemantics tableSemantics(final SpecializedContext context) {
        return CallContextUtils.getTableSemanticsOrThrow(context.getCallContext(), ARG_INPUT);
    }

    /**
     * Returns whether a new record replaces the current one, given the order of the new record
     * relative to the current one: with {@code last} the later arrival wins on a tie, with {@code
     * first} only a strictly smaller record replaces.
     */
    public static boolean replaces(final Keep keep, final int order) {
        return keep == Keep.LAST ? order >= 0 : order < 0;
    }

    /**
     * Compares two records by their {@code sequence_by} values in ascending order. The first column
     * decides, later ones break ties, and NULL orders below every other value.
     */
    @Internal
    public static final class SequenceComparator implements Serializable {

        private static final long serialVersionUID = 1L;

        private final int[] indices;

        private final GeneratedComparator comparator;

        private transient ProjectedRowData projectedLeft;

        private transient ProjectedRowData projectedRight;

        /** Creates a comparator of the columns at {@code indices}, which have the given types. */
        public SequenceComparator(
                final SpecializedContext context, final int[] indices, final DataType[] types) {
            this.indices = indices;
            this.comparator =
                    context.createComparator(
                            DataTypes.ROW(types).toInternal(), SortDirection.ASC_NULLS_FIRST);
        }

        public void open(final FunctionContext context) {
            comparator.open(context);
            projectedLeft = ProjectedRowData.from(indices);
            projectedRight = ProjectedRowData.from(indices);
        }

        /**
         * Returns a negative number, zero, or a positive number if the left record orders before,
         * equal to, or after the right one.
         */
        public int compare(final RowData left, final RowData right) {
            projectedLeft.replaceRow(left);
            projectedRight.replaceRow(right);
            return comparator.compare(projectedLeft, projectedRight);
        }
    }

    private DeduplicateUtils() {}
}
