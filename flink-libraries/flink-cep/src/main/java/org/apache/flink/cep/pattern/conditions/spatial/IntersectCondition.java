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

package org.apache.flink.cep.pattern.conditions.spatial;

import org.apache.flink.annotation.Internal;
import org.apache.flink.cep.pattern.conditions.RichIterativeCondition;
import org.apache.flink.cep.pattern.spatial.GeometryEvent;

import java.util.Optional;

/**
 * A {@link IntersectCondition condition} which checks if the event intersects with previous event
 * returns {@code true} if the original condition returns {@code false}.
 *
 * @param <T> Type of the element to filter
 */
@Internal
public class IntersectCondition<T extends GeometryEvent> extends RichIterativeCondition<T> {
    private static final long serialVersionUID = 1L;

    private final String prevPatternName;
    private final IntersectType intersectType;

    private final Optional<Integer> intersectionCount;

    public IntersectCondition(
            String prevPatternName,
            IntersectType intersectType,
            Optional<Integer> intersectionCount) {
        this.prevPatternName = prevPatternName;
        this.intersectType = intersectType;
        this.intersectionCount = intersectionCount;
    }

    public IntersectCondition(
            String prevPatternName, IntersectType intersectType, int intersectionCount)
            throws Exception {
        this(prevPatternName, intersectType, Optional.of(intersectionCount));
        if (intersectType == IntersectType.INTERSECT_ALL) {
            throw new Exception("Intersection count not required for intersect all type");
        }
        if (intersectionCount <= 0) {
            throw new Exception("Intersection count should be greater than zero");
        }
    }

    public IntersectCondition(String prevPatternName, IntersectType intersectType)
            throws Exception {
        this(prevPatternName, intersectType, Optional.empty());
        if (intersectType != IntersectType.INTERSECT_ALL) {
            throw new Exception("Intersection count cannot be empty for given intersect type");
        }
    }

    @Override
    public boolean filter(T value, Context<T> ctx) throws Exception {
        // TODO --> Add handling for events which doesn't have geometry for ANY_N intersect type
        boolean result;
        int intersectionCountTillNow = 0;
        switch (this.intersectType) {
            case INTERSECT_ALL:
                result = true;
                for (T event : ctx.getEventsForPattern(prevPatternName)) {
                    result = event.getGeometry().intersects(value.getGeometry());
                    if (!result) {
                        return false;
                    }
                }
                break;
            case INTERSECT_ANY_N:
            case INTERSECT_EXACTLY_N:
                result = false;
                for (T event : ctx.getEventsForPattern(prevPatternName)) {
                    if (event.getGeometry().intersects(value.getGeometry())) {
                        intersectionCountTillNow++;
                    }
                    if (intersectType == IntersectType.INTERSECT_ANY_N
                            && intersectionCountTillNow >= intersectionCount.get()) {
                        return true;
                    }
                }
                if (intersectionCountTillNow == intersectionCount.get()) {
                    result = true;
                }
                break;
            case INTERSECT_INORDER_N:
            case INTERSECT_INORDER_EXACTLY_N:
                result = false;
                for (T event : ctx.getEventsForPattern(prevPatternName)) {
                    if (event.getGeometry().intersects(value.getGeometry())) {
                        intersectionCountTillNow++;
                    } else {
                        if (intersectType == IntersectType.INTERSECT_INORDER_EXACTLY_N
                                && intersectionCountTillNow == intersectionCount.get()) {
                            result = true;
                        }
                        intersectionCountTillNow = 0;
                    }
                    if (intersectType == IntersectType.INTERSECT_INORDER_N
                            && intersectionCountTillNow >= intersectionCount.get()) {
                        return true;
                    }
                }
                if (intersectionCountTillNow == intersectionCount.get()) {
                    return true;
                }
                break;
            default:
                result = true;
        }
        return result;
    }
}
