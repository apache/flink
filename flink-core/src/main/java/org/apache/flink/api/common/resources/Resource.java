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

package org.apache.flink.api.common.resources;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Base class for resources one can specify. */
@Internal
public abstract class Resource<T extends Resource<T>>
        implements Serializable, Comparable<Resource> {

    private static final long serialVersionUID = 1L;

    private final String name;

    private final BigDecimal value;

    protected Resource(String name, double value) {
        this(name, BigDecimal.valueOf(value));
    }

    protected Resource(String name, BigDecimal value) {
        checkNotNull(value);
        checkArgument(
                value.compareTo(BigDecimal.ZERO) >= 0, "Resource value must be no less than 0");

        this.name = checkNotNull(name);
        this.value = value;
    }

    public T merge(T other) {
        checkNotNull(other, "Cannot merge with null resources");
        checkArgument(getClass() == other.getClass(), "Merge with different resource type");
        checkArgument(name.equals(other.getName()), "Merge with different resource name");

        return create(value.add(other.getValue()));
    }

    public T subtract(T other) {
        checkNotNull(other, "Cannot subtract null resources");
        checkArgument(getClass() == other.getClass(), "Minus with different resource type");
        checkArgument(name.equals(other.getName()), "Minus with different resource name");
        checkArgument(
                value.compareTo(other.getValue()) >= 0,
                "Try to subtract a larger resource from this one.");

        return create(value.subtract(other.getValue()));
    }

    public T multiply(BigDecimal multiplier) {
        return create(value.multiply(multiplier));
    }

    public T multiply(int multiplier) {
        return multiply(BigDecimal.valueOf(multiplier));
    }

    public T divide(BigDecimal by) {
        return create(value.divide(by, 16, RoundingMode.DOWN));
    }

    public T divide(int by) {
        return divide(BigDecimal.valueOf(by));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && getClass() == o.getClass()) {
            @SuppressWarnings("unchecked")
            T other = (T) o;

            // Two Resources are considered equal if the values equals disregarding the scales
            return name.equals(other.getName()) && value.compareTo(other.getValue()) == 0;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + Objects.hashCode(value.doubleValue());
        return result;
    }

    @Override
    public String toString() {
        return String.format("Resource(%s: %s)", name, value);
    }

    @Override
    public int compareTo(Resource other) {
        checkArgument(other != null && getClass() == other.getClass() && name.equals(other.name));
        return value.compareTo(other.value);
    }

    public String getName() {
        return name;
    }

    public BigDecimal getValue() {
        return value;
    }

    public boolean isZero() {
        return value.compareTo(BigDecimal.ZERO) == 0;
    }

    /**
     * Create a new instance of the sub resource.
     *
     * @param value The value of the resource
     * @return A new instance of the sub resource
     */
    protected abstract T create(BigDecimal value);
}
