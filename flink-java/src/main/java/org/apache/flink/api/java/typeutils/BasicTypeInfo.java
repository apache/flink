/**
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

package org.apache.flink.api.java.typeutils;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanComparator;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ByteComparator;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.CharComparator;
import org.apache.flink.api.common.typeutils.base.CharSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleComparator;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatComparator;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongComparator;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.ShortComparator;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.types.TypeInformation;


/**
 *
 */
public class BasicTypeInfo<T> extends TypeInformation<T> implements AtomicType<T> {

	public static final BasicTypeInfo<String> STRING_TYPE_INFO = new BasicTypeInfo<String>(String.class, StringSerializer.INSTANCE, StringComparator.class);
	public static final BasicTypeInfo<Boolean> BOOLEAN_TYPE_INFO = new BasicTypeInfo<Boolean>(Boolean.class, BooleanSerializer.INSTANCE, BooleanComparator.class);
	public static final BasicTypeInfo<Byte> BYTE_TYPE_INFO = new BasicTypeInfo<Byte>(Byte.class, ByteSerializer.INSTANCE, ByteComparator.class);
	public static final BasicTypeInfo<Short> SHORT_TYPE_INFO = new BasicTypeInfo<Short>(Short.class, ShortSerializer.INSTANCE, ShortComparator.class);
	public static final BasicTypeInfo<Integer> INT_TYPE_INFO = new BasicTypeInfo<Integer>(Integer.class, IntSerializer.INSTANCE, IntComparator.class);
	public static final BasicTypeInfo<Long> LONG_TYPE_INFO = new BasicTypeInfo<Long>(Long.class, LongSerializer.INSTANCE, LongComparator.class);
	public static final BasicTypeInfo<Float> FLOAT_TYPE_INFO = new BasicTypeInfo<Float>(Float.class, FloatSerializer.INSTANCE, FloatComparator.class);
	public static final BasicTypeInfo<Double> DOUBLE_TYPE_INFO = new BasicTypeInfo<Double>(Double.class, DoubleSerializer.INSTANCE, DoubleComparator.class);
	public static final BasicTypeInfo<Character> CHAR_TYPE_INFO = new BasicTypeInfo<Character>(Character.class, CharSerializer.INSTANCE, CharComparator.class);
	
	// --------------------------------------------------------------------------------------------

	private final Class<T> clazz;
	
	private final TypeSerializer<T> serializer;
	
	private final Class<? extends TypeComparator<T>> comparatorClass;
	
	
	private BasicTypeInfo(Class<T> clazz, TypeSerializer<T> serializer, Class<? extends TypeComparator<T>> comparatorClass) {
		this.clazz = clazz;
		this.serializer = serializer;
		this.comparatorClass = comparatorClass;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean isBasicType() {
		return true;
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
		return this.clazz;
	}
	
	@Override
	public boolean isKeyType() {
		return true;
	}
	
	@Override
	public TypeSerializer<T> createSerializer() {
		return this.serializer;
	}
	
	@Override
	public TypeComparator<T> createComparator(boolean sortOrderAscending) {
		return instantiateComparator(comparatorClass, sortOrderAscending);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return this.clazz.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof BasicTypeInfo) {
			@SuppressWarnings("unchecked")
			BasicTypeInfo<T> other = (BasicTypeInfo<T>) obj;
			return this.clazz.equals(other.clazz);
		} else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return clazz.getSimpleName();
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static <X> BasicTypeInfo<X> getInfoFor(Class<X> type) {
		if (type == null) {
			throw new NullPointerException();
		}
		
		@SuppressWarnings("unchecked")
		BasicTypeInfo<X> info = (BasicTypeInfo<X>) TYPES.get(type);
		return info;
	}
	
	private static <X> TypeComparator<X> instantiateComparator(Class<? extends TypeComparator<X>> comparatorClass, boolean ascendingOrder) {
		try {
			Constructor<? extends TypeComparator<X>> constructor = comparatorClass.getConstructor(boolean.class);
			return constructor.newInstance(ascendingOrder);
		}
		catch (Exception e) {
			throw new RuntimeException("Could not initialize basic comparator " + comparatorClass.getName(), e);
		}
	}
	
	private static final Map<Class<?>, BasicTypeInfo<?>> TYPES = new HashMap<Class<?>, BasicTypeInfo<?>>();
	
	static {
		TYPES.put(String.class, STRING_TYPE_INFO);
		TYPES.put(Boolean.class, BOOLEAN_TYPE_INFO);
		TYPES.put(boolean.class, BOOLEAN_TYPE_INFO);
		TYPES.put(Byte.class, BYTE_TYPE_INFO);
		TYPES.put(byte.class, BYTE_TYPE_INFO);
		TYPES.put(Short.class, SHORT_TYPE_INFO);
		TYPES.put(short.class, SHORT_TYPE_INFO);
		TYPES.put(Integer.class, INT_TYPE_INFO);
		TYPES.put(int.class, INT_TYPE_INFO);
		TYPES.put(Long.class, LONG_TYPE_INFO);
		TYPES.put(long.class, LONG_TYPE_INFO);
		TYPES.put(Float.class, FLOAT_TYPE_INFO);
		TYPES.put(float.class, FLOAT_TYPE_INFO);
		TYPES.put(Double.class, DOUBLE_TYPE_INFO);
		TYPES.put(double.class, DOUBLE_TYPE_INFO);
		TYPES.put(Character.class, CHAR_TYPE_INFO);
		TYPES.put(char.class, CHAR_TYPE_INFO);
	}
}
