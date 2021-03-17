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

package org.apache.flink.table.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongComparator;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.table.api.DataTypes;

import java.lang.reflect.Constructor;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Type information for SQL INTERVAL types.
 *
 * @deprecated This class will be removed in future versions as it is used for the old type system.
 *     It is recommended to use {@link DataTypes} instead. Please make sure to use either the old or
 *     the new type system consistently to avoid unintended behavior. See the website documentation
 *     for more information.
 */
@Internal
@Deprecated
public final class TimeIntervalTypeInfo<T> extends TypeInformation<T> implements AtomicType<T> {

    private static final long serialVersionUID = -1816179424364825258L;

    public static final TimeIntervalTypeInfo<Integer> INTERVAL_MONTHS =
            new TimeIntervalTypeInfo<>(Integer.class, IntSerializer.INSTANCE, IntComparator.class);

    public static final TimeIntervalTypeInfo<Long> INTERVAL_MILLIS =
            new TimeIntervalTypeInfo<>(Long.class, LongSerializer.INSTANCE, LongComparator.class);

    private final Class<T> clazz;
    private final TypeSerializer<T> serializer;
    private final Class<? extends TypeComparator<T>> comparatorClass;

    private TimeIntervalTypeInfo(
            Class<T> clazz,
            TypeSerializer<T> serializer,
            Class<? extends TypeComparator<T>> comparatorClass) {
        this.clazz = checkNotNull(clazz);
        this.serializer = checkNotNull(serializer);
        this.comparatorClass = checkNotNull(comparatorClass);
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<T> getTypeClass() {
        return clazz;
    }

    @Override
    public boolean isKeyType() {
        return true;
    }

    @Override
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        return serializer;
    }

    @Override
    public TypeComparator<T> createComparator(
            boolean sortOrderAscending, ExecutionConfig executionConfig) {
        try {
            Constructor<? extends TypeComparator<T>> constructor =
                    comparatorClass.getConstructor(Boolean.TYPE);
            return constructor.newInstance(sortOrderAscending);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not initialize comparator " + comparatorClass.getName(), e);
        }
    }

    // ----------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return Objects.hash(clazz, serializer, comparatorClass);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof TimeIntervalTypeInfo;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj instanceof TimeIntervalTypeInfo) {
            TimeIntervalTypeInfo other = (TimeIntervalTypeInfo) obj;
            return other.canEqual(this)
                    && this.clazz.equals(other.clazz)
                    && serializer == other.serializer
                    && this.comparatorClass.equals(other.comparatorClass);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return TimeIntervalTypeInfo.class.getSimpleName() + "(" + clazz.getSimpleName() + ")";
    }
}
