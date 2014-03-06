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

import eu.stratosphere.api.common.typeutils.Serializer;


public class ArrayTypeInfo<T, C> extends TypeInformation<T> {

	public static final ArrayTypeInfo<String[], String> STRING_ARRAY_TYPE_INFO = new ArrayTypeInfo<String[], String>(String[].class);
	public static final ArrayTypeInfo<Boolean[], Boolean> BOOLEAN_ARRAY_TYPE_INFO = new ArrayTypeInfo<Boolean[], Boolean>(Boolean[].class);
	public static final ArrayTypeInfo<Byte[], Byte> BYTE_ARRAY_TYPE_INFO = new ArrayTypeInfo<Byte[], Byte>(Byte[].class);
	public static final ArrayTypeInfo<Short[], Short> SHORT_ARRAY_TYPE_INFO = new ArrayTypeInfo<Short[], Short>(Short[].class);
	public static final ArrayTypeInfo<Integer[], Integer> INT_ARRAY_TYPE_INFO = new ArrayTypeInfo<Integer[], Integer>(Integer[].class);
	public static final ArrayTypeInfo<Long[], Long> LONG_ARRAY_TYPE_INFO = new ArrayTypeInfo<Long[], Long>(Long[].class);
	public static final ArrayTypeInfo<Float[], Float> FLOAT_ARRAY_TYPE_INFO = new ArrayTypeInfo<Float[], Float>(Float[].class);
	public static final ArrayTypeInfo<Double[], Double> DOUBLE_ARRAY_TYPE_INFO = new ArrayTypeInfo<Double[], Double>(Double[].class);
	public static final ArrayTypeInfo<Character[], Character> CHAR_ARRAY_TYPE_INFO = new ArrayTypeInfo<Character[], Character>(Character[].class);
	
	// --------------------------------------------------------------------------------------------

	private final Class<T> arrayClass;
	private final Class<C> componentClass;
	
	@SuppressWarnings("unchecked")
	private ArrayTypeInfo(Class<T> arrayClass) {
		if (!arrayClass.isArray()) {
			throw new IllegalArgumentException();
		}
		
		this.arrayClass = arrayClass;
		this.componentClass = (Class<C>) arrayClass.getComponentType();
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
	
	public Class<C> getComponentTypeClass() {
		return this.componentClass;
	}
	
	@Override
	public boolean isKeyType() {
		return false;
	}
	
	@Override
	public Serializer<T> createSerializer() {
		throw new UnsupportedOperationException("Array serialization is currently not implemented.");
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static <X, C> ArrayTypeInfo<X, C> getInfoFor(Class<X> type) {
		if (type == null)
			throw new NullPointerException();
		
		@SuppressWarnings("unchecked")
		ArrayTypeInfo<X, C> info = (ArrayTypeInfo<X, C>) TYPES.get(type);
		return info;
	}
	
	private static final Map<Class<?>, ArrayTypeInfo<?, ?>> TYPES = new HashMap<Class<?>, ArrayTypeInfo<?, ?>>();
	
	static {
		TYPES.put(String[].class, STRING_ARRAY_TYPE_INFO);
		TYPES.put(Boolean[].class, BOOLEAN_ARRAY_TYPE_INFO);
		TYPES.put(Byte[].class, BYTE_ARRAY_TYPE_INFO);
		TYPES.put(Short[].class, SHORT_ARRAY_TYPE_INFO);
		TYPES.put(Integer[].class, INT_ARRAY_TYPE_INFO);
		TYPES.put(Long[].class, LONG_ARRAY_TYPE_INFO);
		TYPES.put(Float[].class, FLOAT_ARRAY_TYPE_INFO);
		TYPES.put(Double[].class, DOUBLE_ARRAY_TYPE_INFO);
		TYPES.put(Character[].class, CHAR_ARRAY_TYPE_INFO);
	}
}
