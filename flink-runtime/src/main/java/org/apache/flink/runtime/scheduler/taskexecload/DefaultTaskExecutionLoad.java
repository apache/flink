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

package org.apache.flink.runtime.scheduler.taskexecload;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/** The default implementation of {@link TaskExecutionLoad}. */
@Internal
public class DefaultTaskExecutionLoad implements TaskExecutionLoad {

    public static final TaskExecutionLoad EMPTY = new DefaultTaskExecutionLoad(0f);

    private final float loadValue;

    public DefaultTaskExecutionLoad(float loadValue) {
        checkArgument(loadValue >= 0.0f);
        this.loadValue = loadValue;
    }

    @Override
    public float getLoadValue() {
        return loadValue;
    }

    @Override
    public int getLoadValueAsInt() {
        return (int) loadValue;
    }

    @Override
    public TaskExecutionLoad merge(TaskExecutionLoad other) {
        return other == null
                ? this
                : new DefaultTaskExecutionLoad(loadValue + other.getLoadValue());
    }

    @Override
    public int compareTo(TaskExecutionLoad o) {
        return Float.compare(loadValue, o.getLoadValue());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultTaskExecutionLoad that = (DefaultTaskExecutionLoad) o;
        return compareTo(that) == 0f;
    }

    @Override
    public int hashCode() {
        return Objects.hash(loadValue);
    }

    @Override
    public String toString() {
        return "DefaultTaskExecutionLoad{load=" + loadValue + '}';
    }
}
