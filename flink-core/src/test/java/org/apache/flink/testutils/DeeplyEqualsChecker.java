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

package org.apache.flink.testutils;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Deep equality checker for tests. It performs deep checks for objects which have no proper
 * deepEquals methods like:
 *
 * <ul>
 *   <li>{@link Tuple}s
 *   <li>Java arrays
 *   <li>{@link Throwable}
 * </ul>
 *
 * <p>One can also provide custom check for additional categories of objects with {@link
 * DeeplyEqualsChecker#withCustomCheck(BiFunction, CustomEqualityChecker)}. This is used e.g. in
 * scala's tests.
 */
public class DeeplyEqualsChecker {

    /**
     * Checker that compares o1 and o2 objects if they are deeply equal.
     *
     * <p><b>NOTE:</b> All nested comparisons should be done through checker.
     */
    public interface CustomEqualityChecker {
        boolean check(Object o1, Object o2, DeeplyEqualsChecker checker);
    }

    private final List<Tuple2<BiFunction<Object, Object, Boolean>, CustomEqualityChecker>>
            customCheckers = new ArrayList<>();

    /**
     * Adds custom check. Those check are always performed first, only after that it fallbacks to
     * default checks.
     *
     * @param shouldCheck function to evaluate if the objects should be compared with comparator
     * @param comparator to perform equality comparison if the shouldCheck passed
     * @return checker with added custom checks
     */
    public DeeplyEqualsChecker withCustomCheck(
            BiFunction<Object, Object, Boolean> shouldCheck, CustomEqualityChecker comparator) {
        customCheckers.add(Tuple2.of(shouldCheck, comparator));
        return this;
    }

    public boolean deepEquals(Object o1, Object o2) {
        if (o1 == o2) {
            return true;
        } else if (o1 == null || o2 == null) {
            return false;
        } else {
            return customCheck(o1, o2).orElseGet(() -> deepEquals0(o1, o2));
        }
    }

    private Optional<Boolean> customCheck(Object o1, Object o2) {
        return customCheckers.stream()
                .filter(checker -> checker.f0.apply(o1, o2))
                .findAny()
                .map(checker -> checker.f1.check(o1, o2, this));
    }

    private boolean deepEquals0(Object e1, Object e2) {
        if (e1.getClass().isArray() && e2.getClass().isArray()) {
            return deepEqualsArray(e1, e2);
        } else if (e1 instanceof Tuple && e2 instanceof Tuple) {
            return deepEqualsTuple((Tuple) e1, (Tuple) e2);
        } else if (e1 instanceof Throwable && e2 instanceof Throwable) {
            return ((Throwable) e1).getMessage().equals(((Throwable) e2).getMessage());
        } else {
            return e1.equals(e2);
        }
    }

    private boolean deepEqualsTuple(Tuple tuple1, Tuple tuple2) {
        if (tuple1.getArity() != tuple2.getArity()) {
            return false;
        }

        for (int i = 0; i < tuple1.getArity(); i++) {
            Object o1 = tuple1.getField(i);
            Object o2 = tuple2.getField(i);

            if (!deepEquals(o1, o2)) {
                return false;
            }
        }

        return true;
    }

    private boolean deepEqualsArray(Object array1, Object array2) {
        int length1 = Array.getLength(array1);
        int length2 = Array.getLength(array2);

        if (length1 != length2) {
            return false;
        }

        for (int i = 0; i < length1; i++) {
            Object o1 = Array.get(array1, i);
            Object o2 = Array.get(array2, i);

            if (!deepEquals(o1, o2)) {
                return false;
            }
        }

        return true;
    }
}
