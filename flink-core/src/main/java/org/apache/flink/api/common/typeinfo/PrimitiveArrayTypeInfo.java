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

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BooleanPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.CharPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.DoublePrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.FloatPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.IntPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.LongPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.ShortPrimitiveArraySerializer;

/**
 * A {@link TypeInformation} for arrays of primitive types (int, long, double, ...).
 * Supports the creation of dedicated efficient serializers for these types.
 *
 * @param <T> The type represented by this type information, e.g., int[], double[], long[]
 */
public class PrimitiveArrayTypeInfo<T> extends TypeInformation<T> {
	
	private static final long serialVersionUID = 1L;

	public static final PrimitiveArrayTypeInfo<boolean[]> BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO = new PrimitiveArrayTypeInfo<boolean[]>(boolean[].class, BooleanPrimitiveArraySerializer.INSTANCE);
	public static final PrimitiveArrayTypeInfo<byte[]> BYTE_PRIMITIVE_ARRAY_TYPE_INFO = new PrimitiveArrayTypeInfo<byte[]>(byte[].class, BytePrimitiveArraySerializer.INSTANCE);
	public static final PrimitiveArrayTypeInfo<short[]> SHORT_PRIMITIVE_ARRAY_TYPE_INFO = new PrimitiveArrayTypeInfo<short[]>(short[].class, ShortPrimitiveArraySerializer.INSTANCE);
	public static final PrimitiveArrayTypeInfo<int[]> INT_PRIMITIVE_ARRAY_TYPE_INFO = new PrimitiveArrayTypeInfo<int[]>(int[].class, IntPrimitiveArraySerializer.INSTANCE);
	public static final PrimitiveArrayTypeInfo<long[]> LONG_PRIMITIVE_ARRAY_TYPE_INFO = new PrimitiveArrayTypeInfo<long[]>(long[].class, LongPrimitiveArraySerializer.INSTANCE);
	public static final PrimitiveArrayTypeInfo<float[]> FLOAT_PRIMITIVE_ARRAY_TYPE_INFO = new PrimitiveArrayTypeInfo<float[]>(float[].class, FloatPrimitiveArraySerializer.INSTANCE);
	public static final PrimitiveArrayTypeInfo<double[]> DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO = new PrimitiveArrayTypeInfo<double[]>(double[].class, DoublePrimitiveArraySerializer.INSTANCE);
	public static final PrimitiveArrayTypeInfo<char[]> CHAR_PRIMITIVE_ARRAY_TYPE_INFO = new PrimitiveArrayTypeInfo<char[]>(char[].class, CharPrimitiveArraySerializer.INSTANCE);
	
	// --------------------------------------------------------------------------------------------
	
	/** The class of the array (such as int[].class) */
	private final Class<T> arrayClass;
	
	/** The serializer for the array */
	private final TypeSerializer<T> serializer;

	/**
	 * Creates a new type info for a 
	 * @param arrayClass The class of the array (such as int[].class)
	 * @param serializer The serializer for the array.
	 */
	private PrimitiveArrayTypeInfo(Class<T> arrayClass, TypeSerializer<T> serializer) {
		if (arrayClass == null || serializer == null) {
			throw new NullPointerException();
		}
		if (!(arrayClass.isArray() && arrayClass.getComponentType().isPrimitive())) {
			throw new IllegalArgumentException("Class must represent an array of primitives.");
		}
		this.arrayClass = arrayClass;
		this.serializer = serializer;
	}

	// --------------------------------------------------------------------------------------------

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
		return this.arrayClass;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
		return this.serializer;
	}
	
	@Override
	public String toString() {
		return arrayClass.getComponentType().getName() + "[]";
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof PrimitiveArrayTypeInfo) {
			PrimitiveArrayTypeInfo otherArray = (PrimitiveArrayTypeInfo) other;
			return otherArray.arrayClass == arrayClass;
		}
		return false;
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Tries to get the PrimitiveArrayTypeInfo for an array. Returns null, if the type is an array,
	 * but the component type is not a primitive type.
	 * 
	 * @param type The class of the array.
	 * @return The corresponding PrimitiveArrayTypeInfo, or null, if the array is not an array of primitives.
	 * @throws InvalidTypesException Thrown, if the given class does not represent an array.
	 */
	@SuppressWarnings("unchecked")
	public static <X> PrimitiveArrayTypeInfo<X> getInfoFor(Class<X> type) {
		if (!type.isArray()) {
			throw new InvalidTypesException("The given class is no array.");
		}

		// basic type arrays
		return (PrimitiveArrayTypeInfo<X>) TYPES.get(type);
	}

	/** Static map from array class to type info */
	private static final Map<Class<?>, PrimitiveArrayTypeInfo<?>> TYPES = new HashMap<Class<?>, PrimitiveArrayTypeInfo<?>>();

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
}
