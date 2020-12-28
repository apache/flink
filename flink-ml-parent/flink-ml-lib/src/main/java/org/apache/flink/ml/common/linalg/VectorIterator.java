/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.linalg;

import java.io.Serializable;
import java.util.Iterator;

/**
 * An iterator over the elements of a vector.
 *
 * <p>Usage: <code>
 * Vector vector = ...;
 * VectorIterator iterator = vector.iterator();
 *
 * while(iterator.hasNext()) {
 *     int index = iterator.getIndex();
 *     double value = iterator.getValue();
 *     iterator.next();
 * }
 * </code>
 */
public interface VectorIterator extends Serializable {

    /**
     * Returns {@code true} if the iteration has more elements. Otherwise, {@code false} will be
     * returned.
     *
     * @return {@code true} if the iteration has more elements
     */
    boolean hasNext();

    /**
     * Trigger the cursor points to the next element of the vector.
     *
     * <p>The {@link #getIndex()} while returns the index of the element which the cursor points.
     * The {@link #getValue()} ()} while returns the value of the element which the cursor points.
     *
     * <p>The difference to the {@link Iterator#next()} is that this can avoid the return of boxed
     * type.
     */
    void next();

    /**
     * Returns the index of the element which the cursor points.
     *
     * @returnthe the index of the element which the cursor points.
     */
    int getIndex();

    /**
     * Returns the value of the element which the cursor points.
     *
     * @returnthe the value of the element which the cursor points.
     */
    double getValue();
}
