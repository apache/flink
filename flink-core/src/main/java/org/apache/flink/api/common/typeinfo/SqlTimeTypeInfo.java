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

package org.apache.flink.api.common.typeinfo;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.DateComparator;
import org.apache.flink.api.common.typeutils.base.SqlDateSerializer;
import org.apache.flink.api.common.typeutils.base.SqlTimeSerializer;
import org.apache.flink.api.common.typeutils.base.SqlTimestampComparator;
import org.apache.flink.api.common.typeutils.base.SqlTimestampSerializer;

import java.lang.reflect.Constructor;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Type information for Java SQL Date/Time/Timestamp. */
@PublicEvolving
public class SqlTimeTypeInfo<T> extends TypeInformation<T> implements AtomicType<T> {

    private static final long serialVersionUID = -132955295409131880L;

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static final SqlTimeTypeInfo<Date> DATE =
            new SqlTimeTypeInfo<>(
                    Date.class, SqlDateSerializer.INSTANCE, (Class) DateComparator.class);

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static final SqlTimeTypeInfo<Time> TIME =
            new SqlTimeTypeInfo<>(
                    Time.class, SqlTimeSerializer.INSTANCE, (Class) DateComparator.class);

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static final SqlTimeTypeInfo<Timestamp> TIMESTAMP =
            new SqlTimeTypeInfo<>(
                    Timestamp.class,
                    SqlTimestampSerializer.INSTANCE,
                    (Class) SqlTimestampComparator.class);

    // --------------------------------------------------------------------------------------------

    private final Class<T> clazz;

    private final TypeSerializer<T> serializer;

    private final Class<? extends TypeComparator<T>> comparatorClass;

    protected SqlTimeTypeInfo(
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
    public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
        return serializer;
    }

    @Override
    public TypeComparator<T> createComparator(
            boolean sortOrderAscending, ExecutionConfig executionConfig) {
        return instantiateComparator(comparatorClass, sortOrderAscending);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return Objects.hash(clazz, serializer, comparatorClass);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof SqlTimeTypeInfo;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SqlTimeTypeInfo) {
            @SuppressWarnings("unchecked")
            SqlTimeTypeInfo<T> other = (SqlTimeTypeInfo<T>) obj;

            return other.canEqual(this)
                    && this.clazz == other.clazz
                    && serializer.equals(other.serializer)
                    && this.comparatorClass == other.comparatorClass;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return clazz.getSimpleName();
    }

    // --------------------------------------------------------------------------------------------

    private static <X> TypeComparator<X> instantiateComparator(
            Class<? extends TypeComparator<X>> comparatorClass, boolean ascendingOrder) {
        try {
            Constructor<? extends TypeComparator<X>> constructor =
                    comparatorClass.getConstructor(boolean.class);
            return constructor.newInstance(ascendingOrder);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not initialize comparator " + comparatorClass.getName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <X> SqlTimeTypeInfo<X> getInfoFor(Class<X> type) {
        checkNotNull(type);

        if (type == Date.class) {
            return (SqlTimeTypeInfo<X>) DATE;
        } else if (type == Time.class) {
            return (SqlTimeTypeInfo<X>) TIME;
        } else if (type == Timestamp.class) {
            return (SqlTimeTypeInfo<X>) TIMESTAMP;
        }
        return null;
    }
}
