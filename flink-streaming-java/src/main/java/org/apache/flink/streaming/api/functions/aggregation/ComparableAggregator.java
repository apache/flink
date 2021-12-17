/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;
import org.apache.flink.streaming.util.typeutils.FieldAccessorFactory;

/**
 * An {@link AggregationFunction} that computes values based on comparisons of {@link Comparable
 * Comparables}.
 */
@Internal
public class ComparableAggregator<T> extends AggregationFunction<T> {

    private static final long serialVersionUID = 1L;

    private Comparator comparator;
    private boolean byAggregate;
    private boolean first;
    private final FieldAccessor<T, Object> fieldAccessor;

    private ComparableAggregator(
            AggregationType aggregationType,
            FieldAccessor<T, Object> fieldAccessor,
            boolean first) {
        this.comparator = Comparator.getForAggregation(aggregationType);
        this.byAggregate =
                (aggregationType == AggregationType.MAXBY)
                        || (aggregationType == AggregationType.MINBY);
        this.first = first;
        this.fieldAccessor = fieldAccessor;
    }

    public ComparableAggregator(
            int positionToAggregate,
            TypeInformation<T> typeInfo,
            AggregationType aggregationType,
            ExecutionConfig config) {
        this(positionToAggregate, typeInfo, aggregationType, false, config);
    }

    public ComparableAggregator(
            int positionToAggregate,
            TypeInformation<T> typeInfo,
            AggregationType aggregationType,
            boolean first,
            ExecutionConfig config) {
        this(
                aggregationType,
                FieldAccessorFactory.getAccessor(typeInfo, positionToAggregate, config),
                first);
    }

    public ComparableAggregator(
            String field,
            TypeInformation<T> typeInfo,
            AggregationType aggregationType,
            boolean first,
            ExecutionConfig config) {
        this(aggregationType, FieldAccessorFactory.getAccessor(typeInfo, field, config), first);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T reduce(T value1, T value2) throws Exception {
        Comparable<Object> o1 = (Comparable<Object>) fieldAccessor.get(value1);
        Object o2 = fieldAccessor.get(value2);

        int c = comparator.isExtremal(o1, o2);

        if (byAggregate) {
            // if they are the same we choose based on whether we want to first or last
            // element with the min/max.
            if (c == 0) {
                return first ? value1 : value2;
            }

            return c == 1 ? value1 : value2;

        } else {
            if (c == 0) {
                value1 = fieldAccessor.set(value1, o2);
            }
            return value1;
        }
    }
}
