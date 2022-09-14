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
package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Internal;

/**
 * This interface defines the method required by the runtime to use data types in join-like
 * operations. In such operations, instances of different data types are compared for equality with
 * respect to certain attributes, such as join keys.
 *
 * <p>The class compares instances of two different data types. One is always used as the reference
 * data type, and the other is checked against the reference. An instance of the reference data type
 * is normally set as the reference for comparisons. Afterwards one or more instances of the other
 * data type are checked against the reference.
 *
 * @param <T1> The class of the first data type.
 * @param <T2> The class of the second data type.
 */
@Internal
public abstract class TypePairComparator<T1, T2> {

    /**
     * Sets the reference for comparisons.
     *
     * @param reference The reference instance.
     */
    public abstract void setReference(T1 reference);

    /**
     * Checks, whether the given candidate instance is equal to the reference instance, with respect
     * to this comparator's equality definition.
     *
     * @param candidate The candidate to check.
     * @return True, if the candidate is equal to the reference, false otherwise.
     */
    public abstract boolean equalToReference(T2 candidate);

    public abstract int compareToReference(T2 candidate);
}
