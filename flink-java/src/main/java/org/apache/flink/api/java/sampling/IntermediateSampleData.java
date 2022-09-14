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

package org.apache.flink.api.java.sampling;

import org.apache.flink.annotation.Internal;

/**
 * The data structure which is transferred between partitions and the coordinator for distributed
 * random sampling.
 *
 * @param <T> The type of sample data.
 */
@Internal
public class IntermediateSampleData<T> implements Comparable<IntermediateSampleData<T>> {
    private double weight;
    private T element;

    public IntermediateSampleData(double weight, T element) {
        this.weight = weight;
        this.element = element;
    }

    public double getWeight() {
        return weight;
    }

    public T getElement() {
        return element;
    }

    @Override
    public int compareTo(IntermediateSampleData<T> other) {
        return this.weight >= other.getWeight() ? 1 : -1;
    }
}
