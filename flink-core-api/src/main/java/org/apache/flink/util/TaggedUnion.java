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

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

/**
 * Utility class for implementing CoGroupedStream in DataStream V1, as well as two-input window
 * operations in DataStream V2.
 */
@Internal
public class TaggedUnion<T1, T2> {
    private final T1 one;
    private final T2 two;

    private TaggedUnion(T1 one, T2 two) {
        this.one = one;
        this.two = two;
    }

    public boolean isOne() {
        return one != null;
    }

    public boolean isTwo() {
        return two != null;
    }

    public T1 getOne() {
        return one;
    }

    public T2 getTwo() {
        return two;
    }

    public static <T1, T2> TaggedUnion<T1, T2> one(T1 one) {
        return new TaggedUnion<>(one, null);
    }

    public static <T1, T2> TaggedUnion<T1, T2> two(T2 two) {
        return new TaggedUnion<>(null, two);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof TaggedUnion)) {
            return false;
        }

        TaggedUnion<?, ?> other = (TaggedUnion<?, ?>) obj;
        return Objects.equals(one, other.one) && Objects.equals(two, other.two);
    }
}
