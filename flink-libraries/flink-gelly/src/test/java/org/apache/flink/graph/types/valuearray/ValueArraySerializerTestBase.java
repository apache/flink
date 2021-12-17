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

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import java.util.Objects;

/**
 * Base class for tests of {@link org.apache.flink.api.common.typeutils.TypeSerializer}s for any
 * {@link ValueArray}. It overrides default deepEquals of {@link Iterable}s with {@link
 * Objects#equals(Object, Object)}.
 */
public abstract class ValueArraySerializerTestBase<U extends ValueArray<?>>
        extends SerializerTestBase<U> {
    ValueArraySerializerTestBase() {
        super(
                new DeeplyEqualsChecker()
                        .withCustomCheck(
                                (o1, o2) -> o1 instanceof ValueArray && o2 instanceof ValueArray,
                                (o1, o2, checker) -> Objects.equals(o1, o2)));
    }
}
