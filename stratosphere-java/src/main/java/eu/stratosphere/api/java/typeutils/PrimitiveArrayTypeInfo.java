/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.typeutils;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.base.array.BooleanPrimitiveArraySerializer;
import eu.stratosphere.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import eu.stratosphere.api.common.typeutils.base.array.CharPrimitiveArraySerializer;
import eu.stratosphere.api.common.typeutils.base.array.DoublePrimitiveArraySerializer;
import eu.stratosphere.api.common.typeutils.base.array.FloatPrimitiveArraySerializer;
import eu.stratosphere.api.common.typeutils.base.array.IntPrimitiveArraySerializer;
import eu.stratosphere.api.common.typeutils.base.array.LongPrimitiveArraySerializer;
import eu.stratosphere.api.common.typeutils.base.array.ShortPrimitiveArraySerializer;
import eu.stratosphere.api.java.functions.InvalidTypesException;

public class PrimitiveArrayTypeInfo<T> extends TypeInformation<T> {

	public static final PrimitiveArrayTypeInfo<boolean[]> BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO = new PrimitiveArrayTypeInfo<boolean[]>(boolean[].class, BooleanPrimitiveArraySerializer.INSTANCE);
	public static final PrimitiveArrayTypeInfo<byte[]> BYTE_PRIMITIVE_ARRAY_TYPE_INFO = new PrimitiveArrayTypeInfo<byte[]>(byte[].class, BytePrimitiveArraySerializer.INSTANCE);
	public static final PrimitiveArrayTypeInfo<short[]> SHORT_PRIMITIVE_ARRAY_TYPE_INFO = new PrimitiveArrayTypeInfo<short[]>(short[].class, ShortPrimitiveArraySerializer.INSTANCE);
	public static final PrimitiveArrayTypeInfo<int[]> INT_PRIMITIVE_ARRAY_TYPE_INFO = new PrimitiveArrayTypeInfo<int[]>(int[].class, IntPrimitiveArraySerializer.INSTANCE);
	public static final PrimitiveArrayTypeInfo<long[]> LONG_PRIMITIVE_ARRAY_TYPE_INFO = new PrimitiveArrayTypeInfo<long[]>(long[].class, LongPrimitiveArraySerializer.INSTANCE);
	public static final PrimitiveArrayTypeInfo<float[]> FLOAT_PRIMITIVE_ARRAY_TYPE_INFO = new PrimitiveArrayTypeInfo<float[]>(float[].class, FloatPrimitiveArraySerializer.INSTANCE);
	public static final PrimitiveArrayTypeInfo<double[]> DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO = new PrimitiveArrayTypeInfo<double[]>(double[].class, DoublePrimitiveArraySerializer.INSTANCE);
	public static final PrimitiveArrayTypeInfo<char[]> CHAR_PRIMITIVE_ARRAY_TYPE_INFO = new PrimitiveArrayTypeInfo<char[]>(char[].class, CharPrimitiveArraySerializer.INSTANCE);
	
	// --------------------------------------------------------------------------------------------
	
	private final Class<T> arrayClass;
	private final TypeSerializer<T> serializer;

	private PrimitiveArrayTypeInfo(Class<T> arrayClass, TypeSerializer<T> serializer) {
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
	public Class<T> getTypeClass() {
		return this.arrayClass;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<T> createSerializer() {
		return this.serializer;
	}
	
	@Override
	public String toString() {
		return arrayClass.getComponentType().getName() + "[]";
	}
	
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	public static <X> PrimitiveArrayTypeInfo<X> getInfoFor(Class<X> type) {
		if (!type.isArray()) {
			throw new InvalidTypesException("The given class is no array.");
		}

		// basic type arrays
		return (PrimitiveArrayTypeInfo<X>) TYPES.get(type);
	}

	private static final Map<Class<?>, PrimitiveArrayTypeInfo<?>> TYPES = new HashMap<Class<?>, PrimitiveArrayTypeInfo<?>>();

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
