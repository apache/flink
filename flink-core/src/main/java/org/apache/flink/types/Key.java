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

package org.apache.flink.types;

import org.apache.flink.annotation.PublicEvolving;

/**
 * This interface has to be implemented by all data types that act as key. Keys are used to
 * establish relationships between values. A key must always be {@link java.lang.Comparable} to
 * other keys of the same type. In addition, keys must implement a correct {@link
 * java.lang.Object#hashCode()} method and {@link java.lang.Object#equals(Object)} method to ensure
 * that grouping on keys works properly.
 *
 * <p>This interface extends {@link org.apache.flink.types.Value} and requires to implement the
 * serialization of its value.
 *
 * @see org.apache.flink.types.Value
 * @see org.apache.flink.core.io.IOReadableWritable
 * @see java.lang.Comparable
 * @deprecated The Key type is a relict of a deprecated and removed API and will be removed in
 *     future (2.0) versions as well.
 */
@Deprecated
@PublicEvolving
public interface Key<T> extends Value, Comparable<T> {

    /**
     * All keys must override the hash-code function to generate proper deterministic hash codes,
     * based on their contents.
     *
     * @return The hash code of the key
     */
    public int hashCode();

    /**
     * Compares the object on equality with another object.
     *
     * @param other The other object to compare against.
     * @return True, iff this object is identical to the other object, false otherwise.
     */
    public boolean equals(Object other);
}
