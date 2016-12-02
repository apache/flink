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
import java.util.Objects;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.StringArraySerializer;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeutils.base.GenericArraySerializer;
import org.apache.flink.api.java.typeutils.FieldAccessor;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Public
public final class BasicArrayTypeInfo<T, C> extends TypeInformation<T> {

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
	private final TypeInformation<C> componentInfo;
	
	private BasicArrayTypeInfo(Class<T> arrayClass, BasicTypeInfo<C> componentInfo) {
		this.arrayClass = checkNotNull(arrayClass);
		this.componentInfo = checkNotNull(componentInfo);
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

	@PublicEvolving
	public Class<C> getComponentTypeClass() {
		return this.componentInfo.getTypeClass();
	}

	@PublicEvolving
	public TypeInformation<C> getComponentInfo() {
		return componentInfo;
	}

	@Override
	@PublicEvolving
	public boolean isKeyType() {
		return false;
	}

	@Override
	@SuppressWarnings("unchecked")
	@PublicEvolving
	public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
		// special case the string array
		if (componentInfo.getTypeClass().equals(String.class)) {
			return (TypeSerializer<T>) StringArraySerializer.INSTANCE;
		} else {
			return (TypeSerializer<T>) new GenericArraySerializer<C>(
				this.componentInfo.getTypeClass(),
				this.componentInfo.createSerializer(executionConfig));
		}
	}

	@Override
	@PublicEvolving
	public <F> FieldAccessor<T, F> getFieldAccessor(int pos, ExecutionConfig config) {
		return new FieldAccessor.ArrayFieldAccessor<>(pos, this);
	}

	@Override
	@PublicEvolving
	public <F> FieldAccessor<T, F> getFieldAccessor(String pos, ExecutionConfig config) {
		try {
			return new FieldAccessor.ArrayFieldAccessor<>(Integer.parseInt(pos), this);
		} catch (NumberFormatException ex) {
			throw new InvalidFieldReferenceException
				("A field expression on an array must be an integer index (that might be given as a string).");
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof BasicArrayTypeInfo) {
			BasicArrayTypeInfo<?, ?> other = (BasicArrayTypeInfo<?, ?>) obj;

			return other.canEqual(this) &&
				arrayClass == other.arrayClass &&
				componentInfo.equals(other.componentInfo);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(arrayClass, componentInfo);
	}


	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BasicArrayTypeInfo;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName()+"<"+this.componentInfo+">";
	}

	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@PublicEvolving
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
