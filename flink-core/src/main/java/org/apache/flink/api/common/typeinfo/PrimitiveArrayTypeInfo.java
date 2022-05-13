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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BooleanPrimitiveArrayComparator;
import org.apache.flink.api.common.typeutils.base.array.BooleanPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArrayComparator;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.CharPrimitiveArrayComparator;
import org.apache.flink.api.common.typeutils.base.array.CharPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.DoublePrimitiveArrayComparator;
import org.apache.flink.api.common.typeutils.base.array.DoublePrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.FloatPrimitiveArrayComparator;
import org.apache.flink.api.common.typeutils.base.array.FloatPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.IntPrimitiveArrayComparator;
import org.apache.flink.api.common.typeutils.base.array.IntPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.LongPrimitiveArrayComparator;
import org.apache.flink.api.common.typeutils.base.array.LongPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.PrimitiveArrayComparator;
import org.apache.flink.api.common.typeutils.base.array.ShortPrimitiveArrayComparator;
import org.apache.flink.api.common.typeutils.base.array.ShortPrimitiveArraySerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TypeInformation} for arrays of primitive types (int, long, double, ...). Supports the
 * creation of dedicated efficient serializers for these types.
 *
 * @param <T> The type represented by this type information, e.g., int[], double[], long[]
 */
@Public
public class PrimitiveArrayTypeInfo<T> extends TypeInformation<T> implements AtomicType<T> {

    private static final long serialVersionUID = 1L;

    public static final PrimitiveArrayTypeInfo<boolean[]> BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO =
            new PrimitiveArrayTypeInfo<>(
                    boolean[].class,
                    BooleanPrimitiveArraySerializer.INSTANCE,
                    BooleanPrimitiveArrayComparator.class);
    public static final PrimitiveArrayTypeInfo<byte[]> BYTE_PRIMITIVE_ARRAY_TYPE_INFO =
            new PrimitiveArrayTypeInfo<>(
                    byte[].class,
                    BytePrimitiveArraySerializer.INSTANCE,
                    BytePrimitiveArrayComparator.class);
    public static final PrimitiveArrayTypeInfo<short[]> SHORT_PRIMITIVE_ARRAY_TYPE_INFO =
            new PrimitiveArrayTypeInfo<>(
                    short[].class,
                    ShortPrimitiveArraySerializer.INSTANCE,
                    ShortPrimitiveArrayComparator.class);
    public static final PrimitiveArrayTypeInfo<int[]> INT_PRIMITIVE_ARRAY_TYPE_INFO =
            new PrimitiveArrayTypeInfo<>(
                    int[].class,
                    IntPrimitiveArraySerializer.INSTANCE,
                    IntPrimitiveArrayComparator.class);
    public static final PrimitiveArrayTypeInfo<long[]> LONG_PRIMITIVE_ARRAY_TYPE_INFO =
            new PrimitiveArrayTypeInfo<>(
                    long[].class,
                    LongPrimitiveArraySerializer.INSTANCE,
                    LongPrimitiveArrayComparator.class);
    public static final PrimitiveArrayTypeInfo<float[]> FLOAT_PRIMITIVE_ARRAY_TYPE_INFO =
            new PrimitiveArrayTypeInfo<>(
                    float[].class,
                    FloatPrimitiveArraySerializer.INSTANCE,
                    FloatPrimitiveArrayComparator.class);
    public static final PrimitiveArrayTypeInfo<double[]> DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO =
            new PrimitiveArrayTypeInfo<>(
                    double[].class,
                    DoublePrimitiveArraySerializer.INSTANCE,
                    DoublePrimitiveArrayComparator.class);
    public static final PrimitiveArrayTypeInfo<char[]> CHAR_PRIMITIVE_ARRAY_TYPE_INFO =
            new PrimitiveArrayTypeInfo<>(
                    char[].class,
                    CharPrimitiveArraySerializer.INSTANCE,
                    CharPrimitiveArrayComparator.class);

    // --------------------------------------------------------------------------------------------

    /** The class of the array (such as int[].class). */
    private final Class<T> arrayClass;

    /** The serializer for the array. */
    private final TypeSerializer<T> serializer;

    /** The class of the comparator for the array. */
    private final Class<? extends PrimitiveArrayComparator<T, ?>> comparatorClass;

    /**
     * Creates a new type info for the primitive type array.
     *
     * @param arrayClass The class of the array (such as int[].class)
     * @param serializer The serializer for the array.
     * @param comparatorClass The class of the array comparator
     */
    private PrimitiveArrayTypeInfo(
            Class<T> arrayClass,
            TypeSerializer<T> serializer,
            Class<? extends PrimitiveArrayComparator<T, ?>> comparatorClass) {
        this.arrayClass = checkNotNull(arrayClass);
        this.serializer = checkNotNull(serializer);
        this.comparatorClass = checkNotNull(comparatorClass);

        checkArgument(
                arrayClass.isArray() && arrayClass.getComponentType().isPrimitive(),
                "Class must represent an array of primitives");
    }

    // --------------------------------------------------------------------------------------------

    @Override
    @PublicEvolving
    public boolean isBasicType() {
        return false;
    }

    @Override
    @PublicEvolving
    public boolean isTupleType() {
        return false;
    }

    @Override
    @PublicEvolving
    public int getArity() {
        return 1;
    }

    @Override
    @PublicEvolving
    public int getTotalFields() {
        return 1;
    }

    @Override
    @PublicEvolving
    public Class<T> getTypeClass() {
        return this.arrayClass;
    }

    @Override
    @PublicEvolving
    public boolean isKeyType() {
        return true;
    }

    @Override
    @PublicEvolving
    public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
        return this.serializer;
    }

    /**
     * Gets the class that represents the component type.
     *
     * @return The class of the component type.
     */
    @PublicEvolving
    public Class<?> getComponentClass() {
        return this.arrayClass.getComponentType();
    }

    /**
     * Gets the type information of the component type.
     *
     * @return The type information of the component type.
     */
    @PublicEvolving
    public TypeInformation<?> getComponentType() {
        return BasicTypeInfo.getInfoFor(getComponentClass());
    }

    @Override
    public String toString() {
        return arrayClass.getComponentType().getName() + "[]";
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof PrimitiveArrayTypeInfo) {
            @SuppressWarnings("unchecked")
            PrimitiveArrayTypeInfo<T> otherArray = (PrimitiveArrayTypeInfo<T>) other;

            return otherArray.canEqual(this)
                    && arrayClass == otherArray.arrayClass
                    && serializer.equals(otherArray.serializer)
                    && comparatorClass == otherArray.comparatorClass;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(arrayClass, serializer, comparatorClass);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof PrimitiveArrayTypeInfo;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Tries to get the PrimitiveArrayTypeInfo for an array. Returns null, if the type is an array,
     * but the component type is not a primitive type.
     *
     * @param type The class of the array.
     * @return The corresponding PrimitiveArrayTypeInfo, or null, if the array is not an array of
     *     primitives.
     * @throws InvalidTypesException Thrown, if the given class does not represent an array.
     */
    @SuppressWarnings("unchecked")
    @PublicEvolving
    public static <X> PrimitiveArrayTypeInfo<X> getInfoFor(Class<X> type) {
        if (!type.isArray()) {
            throw new InvalidTypesException("The given class is no array.");
        }

        // basic type arrays
        return (PrimitiveArrayTypeInfo<X>) TYPES.get(type);
    }

    /** Static map from array class to type info. */
    private static final Map<Class<?>, PrimitiveArrayTypeInfo<?>> TYPES = new HashMap<>();

    // initialization of the static map
    static {
        TYPES.put(boolean[].class, BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO);
        TYPES.put(byte[].class, BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        TYPES.put(short[].class, SHORT_PRIMITIVE_ARRAY_TYPE_INFO);
        TYPES.put(int[].class, INT_PRIMITIVE_ARRAY_TYPE_INFO);
        TYPES.put(long[].class, LONG_PRIMITIVE_ARRAY_TYPE_INFO);
        TYPES.put(float[].class, FLOAT_PRIMITIVE_ARRAY_TYPE_INFO);
        TYPES.put(double[].class, DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO);
        TYPES.put(char[].class, CHAR_PRIMITIVE_ARRAY_TYPE_INFO);
    }

    @Override
    @PublicEvolving
    public PrimitiveArrayComparator<T, ?> createComparator(
            boolean sortOrderAscending, ExecutionConfig executionConfig) {
        try {
            return comparatorClass.getConstructor(boolean.class).newInstance(sortOrderAscending);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not initialize primitive "
                            + comparatorClass.getName()
                            + " array comparator.",
                    e);
        }
    }
}
