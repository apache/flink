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

package org.apache.flink.optimizer.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.java.typeutils.runtime.RuntimeComparatorFactory;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.PlanNode;

import java.util.Arrays;

/** Utility class that contains helper methods for optimizer. */
public final class Utils {

    public static FieldList createOrderedFromSet(FieldSet set) {
        if (set instanceof FieldList) {
            return (FieldList) set;
        } else {
            final int[] cols = set.toArray();
            Arrays.sort(cols);
            return new FieldList(cols);
        }
    }

    public static Ordering createOrdering(FieldList fields, boolean[] directions) {
        final Ordering o = new Ordering();
        for (int i = 0; i < fields.size(); i++) {
            o.appendOrdering(
                    fields.get(i),
                    null,
                    directions == null || directions[i] ? Order.ASCENDING : Order.DESCENDING);
        }
        return o;
    }

    public static Ordering createOrdering(FieldList fields) {
        final Ordering o = new Ordering();
        for (int i = 0; i < fields.size(); i++) {
            o.appendOrdering(fields.get(i), null, Order.ANY);
        }
        return o;
    }

    public static boolean[] getDirections(Ordering o, int numFields) {
        final boolean[] dirs = o.getFieldSortDirections();
        if (dirs.length == numFields) {
            return dirs;
        } else if (dirs.length > numFields) {
            final boolean[] subSet = new boolean[numFields];
            System.arraycopy(dirs, 0, subSet, 0, numFields);
            return subSet;
        } else {
            throw new CompilerException();
        }
    }

    public static TypeComparatorFactory<?> getShipComparator(
            Channel channel, ExecutionConfig executionConfig) {
        PlanNode source = channel.getSource();
        Operator<?> javaOp = source.getProgramOperator();
        TypeInformation<?> type = javaOp.getOperatorInfo().getOutputType();
        return createComparator(
                type,
                channel.getShipStrategyKeys(),
                getSortOrders(channel.getShipStrategyKeys(), channel.getShipStrategySortOrder()),
                executionConfig);
    }

    private static <T> TypeComparatorFactory<?> createComparator(
            TypeInformation<T> typeInfo,
            FieldList keys,
            boolean[] sortOrder,
            ExecutionConfig executionConfig) {

        TypeComparator<T> comparator;
        if (typeInfo instanceof CompositeType) {
            comparator =
                    ((CompositeType<T>) typeInfo)
                            .createComparator(keys.toArray(), sortOrder, 0, executionConfig);
        } else if (typeInfo instanceof AtomicType) {
            // handle grouping of atomic types
            comparator = ((AtomicType<T>) typeInfo).createComparator(sortOrder[0], executionConfig);
        } else {
            throw new RuntimeException("Unrecognized type: " + typeInfo);
        }

        return new RuntimeComparatorFactory<>(comparator);
    }

    private static boolean[] getSortOrders(FieldList keys, boolean[] orders) {
        if (orders == null) {
            orders = new boolean[keys.size()];
            Arrays.fill(orders, true);
        }
        return orders;
    }

    // --------------------------------------------------------------------------------------------

    /** No instantiation. */
    private Utils() {
        throw new RuntimeException();
    }
}
