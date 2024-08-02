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

package org.apache.flink.runtime.scheduler.loading;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Objects;

/** The default implementation of {@link LoadingWeight}. */
@Internal
public class DefaultLoadingWeight implements LoadingWeight {

    public static final LoadingWeight EMPTY = new DefaultLoadingWeight(0f);

    private final float loading;

    public DefaultLoadingWeight(float loading) {
        Preconditions.checkArgument(loading >= 0.0f);
        this.loading = loading;
    }

    @Override
    public float getLoading() {
        return loading;
    }

    @Override
    public LoadingWeight merge(LoadingWeight other) {
        if (other == null) {
            return this;
        }
        return new DefaultLoadingWeight(loading + other.getLoading());
    }

    @Override
    public int compareTo(@Nonnull LoadingWeight o) {
        return Float.compare(loading, o.getLoading());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultLoadingWeight that = (DefaultLoadingWeight) o;
        return Float.compare(loading, that.loading) == 0f;
    }

    @Override
    public int hashCode() {
        return Objects.hash(loading);
    }

    @Override
    public String toString() {
        return "DefaultLoadingWeight{loading=" + loading + '}';
    }
}
