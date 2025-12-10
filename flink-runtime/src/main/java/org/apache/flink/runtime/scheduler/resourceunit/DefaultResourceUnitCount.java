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

package org.apache.flink.runtime.scheduler.resourceunit;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/** The default implementation of {@link ResourceUnitCount}. */
@Internal
public class DefaultResourceUnitCount implements ResourceUnitCount {

    public static final ResourceUnitCount EMPTY = new DefaultResourceUnitCount(0f);

    private final float count;

    public DefaultResourceUnitCount(float count) {
        Preconditions.checkArgument(count >= 0.0f);
        this.count = count;
    }

    @Override
    public float getCount() {
        return count;
    }

    @Override
    public int getCountAsInt() {
        return (int) count;
    }

    @Override
    public ResourceUnitCount merge(ResourceUnitCount other) {
        return other == null ? this : new DefaultResourceUnitCount(count + other.getCount());
    }

    @Override
    public int compareTo(ResourceUnitCount o) {
        return Float.compare(count, o.getCount());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultResourceUnitCount that = (DefaultResourceUnitCount) o;
        return compareTo(that) == 0f;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count);
    }

    @Override
    public String toString() {
        return "DefaultResourceUnitCount{count=" + count + '}';
    }
}
