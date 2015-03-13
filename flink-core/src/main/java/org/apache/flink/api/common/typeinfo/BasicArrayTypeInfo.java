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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.StringArraySerializer;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeutils.base.GenericArraySerializer;

public class BasicArrayTypeInfo<T, C> extends TypeInformation<T> {

	private static final long serialVersionUID = 1L;

	public static final BasicArrayTypeInfo<String[], String> STRING_ARRAY_TYPE_INFO = new BasicArrayTypeInfo<String[], String>(String[].class, BasicTypeInfo.STRING_TYPE_INFO);
	
	public static final BasicArrayTypeInfo<Boolean[], Boolean> BOOLEAN_ARRAY_TYPE_INFO = new BasicArrayTypeInfo<Boolean[], Boolean>(Boolean[].class, BasicTypeInfo.BOOLEAN_TYPE_INFO);
	public static final BasicArrayTypeInfo<Byte[], Byte> BYTE_ARRAY_TYPE_INFO = new BasicArrayTypeInfo<Byte[], Byte>(Byte[].class, BasicTypeInfo.BYTE_TYPE_INFO);
	public static final BasicArrayTypeInfo<Short[], Short> SHORT_ARRAY_TYPE_INFO = new BasicArrayTypeInfo<Short[], Short>(Short[].class, BasicTypeInfo.SHORT_TYPE_INFO);
	public static final BasicArrayTypeInfo<Integer[], Integer> INT_ARRAY_TYPE_INFO = new BasicArrayTypeInfo<Integer[], Integer>(Integer[].class, BasicTypeInfo.INT_TYPE_INFO);
	public static final BasicArrayTypeInfo<Long[], Long> LONG_ARRAY_TYPE_INFO = new BasicArrayTypeInfo<Long[], Long>(Long[].class, BasicTypeInfo.LONG_TYPE_INFO);
	public static final BasicArrayTypeInfo<Float[], Float> FLOAT_ARRAY_TYPE_INFO = new BasicArrayTypeInfo<Float[], Float>(Float[].class, BasicTypeInfo.FLOAT_TYPE_INFO);
	public static final BasicArrayTypeInfo<Double[], Double> DOUBLE_ARRAY_TYPE_INFO = new BasicArrayTypeInfo<Double[], Double>(Double[].class, BasicTypeInfo.DOUBLE_TYPE_INFO);
	public static final BasicArrayTypeInfo<Character[], Character> CHAR_ARRAY_TYPE_INFO = new BasicArrayTypeInfo<Character[], Character>(Character[].class, BasicTypeInfo.CHAR_TYPE_INFO);
	
	// --------------------------------------------------------------------------------------------

	private final Class<T> arrayClass;
	private final Class<C> componentClass;
	private final TypeInformation<C> componentInfo;

	@SuppressWarnings("unchecked")
	private BasicArrayTypeInfo(Class<T> arrayClass, BasicTypeInfo<C> componentInfo) {
		this.arrayClass = arrayClass;
		this.componentClass = (Class<C>) arrayClass.getComponentType();
		this.componentInfo = componentInfo;
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

	public Class<C> getComponentTypeClass() {
		return this.componentClass;
	}
	
	public TypeInformation<C> getComponentInfo() {
		return componentInfo;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	@SuppressWarnings("unchecked")
	public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
		// special case the string array
		if (componentClass.equals(String.class)) {
			return (TypeSerializer<T>) StringArraySerializer.INSTANCE;
		} else {
			return (TypeSerializer<T>) new GenericArraySerializer<C>(this.componentClass, this.componentInfo.createSerializer(executionConfig));
		}
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName()+"<"+this.componentInfo+">";
	}

	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	public static <X, C> BasicArrayTypeInfo<X, C> getInfoFor(Class<X> type) {
		if (!type.isArray()) {
			throw new InvalidTypesException("The given class is no array.");
		}

		// basic type arrays
		return (BasicArrayTypeInfo<X, C>) TYPES.get(type);
	}

	private static final Map<Class<?>, BasicArrayTypeInfo<?, ?>> TYPES = new HashMap<Class<?>, BasicArrayTypeInfo<?, ?>>();

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
