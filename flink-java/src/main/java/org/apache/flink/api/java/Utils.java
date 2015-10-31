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

package org.apache.flink.api.java;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.RichFlatMapFunction;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import static org.apache.flink.api.java.functions.FunctionAnnotation.SkipCodeAnalysis;

/**
 * Utility class that contains helper methods to work with Java APIs.
 */
public final class Utils {
	
	public static final Random RNG = new Random();

	public static String getCallLocationName() {
		return getCallLocationName(4);
	}

	public static String getCallLocationName(int depth) {
		StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

		if (stackTrace.length < depth) {
			return "<unknown>";
		}

		StackTraceElement elem = stackTrace[depth];

		return String.format("%s(%s:%d)", elem.getMethodName(), elem.getFileName(), elem.getLineNumber());
	}

	/**
	 * Returns all GenericTypeInfos contained in a composite type.
	 *
	 * @param typeInfo
	 */
	public static void getContainedGenericTypes(CompositeType typeInfo, List<GenericTypeInfo<?>> target) {
		for(int i = 0; i < typeInfo.getArity(); i++) {
			TypeInformation<?> type = typeInfo.getTypeAt(i);
			if(type instanceof CompositeType) {
				getContainedGenericTypes((CompositeType) type, target);
			} else if(type instanceof GenericTypeInfo) {
				if(!target.contains(type)) {
					target.add((GenericTypeInfo<?>) type);
				}
			}
		}
	}

	@SkipCodeAnalysis
	public static class CountHelper<T> extends RichFlatMapFunction<T, Long> {

		private static final long serialVersionUID = 1L;

		private final String id;
		private long counter;

		public CountHelper(String id) {
			this.id = id;
			this.counter = 0L;
		}

		@Override
		public void flatMap(T value, Collector<Long> out) throws Exception {
			counter++;
		}

		@Override
		public void close() throws Exception {
			getRuntimeContext().getLongCounter(id).add(counter);
		}
	}

	@SkipCodeAnalysis
	public static class CollectHelper<T> extends RichFlatMapFunction<T, T> {

		private static final long serialVersionUID = 1L;

		private final String id;
		private final TypeSerializer<T> serializer;
		
		private SerializedListAccumulator<T> accumulator;

		public CollectHelper(String id, TypeSerializer<T> serializer) {
			this.id = id;
			this.serializer = serializer;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.accumulator = new SerializedListAccumulator<T>();
		}

		@Override
		public void flatMap(T value, Collector<T> out) throws Exception {
			accumulator.add(value, serializer);
		}

		@Override
		public void close() throws Exception {
			// Important: should only be added in close method to minimize traffic of accumulators
			getRuntimeContext().addAccumulator(id, accumulator);
		}
	}


	// --------------------------------------------------------------------------------------------

	/**
	 * Debugging utility to understand the hierarchy of serializers created by the Java API.
	 * Tested in GroupReduceITCase.testGroupByGenericType()
	 */
	public static <T> String getSerializerTree(TypeInformation<T> ti) {
		return getSerializerTree(ti, 0);
	}

	private static <T> String getSerializerTree(TypeInformation<T> ti, int indent) {
		String ret = "";
		if(ti instanceof CompositeType) {
			ret += StringUtils.repeat(' ', indent) + ti.getClass().getSimpleName()+"\n";
			CompositeType<T> cti = (CompositeType<T>) ti;
			String[] fieldNames = cti.getFieldNames();
			for(int i = 0; i < cti.getArity(); i++) {
				TypeInformation fieldType = cti.getTypeAt(i);
				ret += StringUtils.repeat(' ', indent + 2) + fieldNames[i]+":"+getSerializerTree(fieldType, indent);
			}
		} else {
			if(ti instanceof GenericTypeInfo) {
				ret += StringUtils.repeat(' ', indent) + "GenericTypeInfo ("+ti.getTypeClass().getSimpleName()+")\n";
				ret += getGenericTypeTree(ti.getTypeClass(), indent + 4);
			} else {
				ret += StringUtils.repeat(' ', indent) + ti.toString()+"\n";
			}
		}
		return ret;
	}

	private static String getGenericTypeTree(Class type, int indent) {
		String ret = "";
		for(Field field : type.getDeclaredFields()) {
			if(Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) {
				continue;
			}
			ret += StringUtils.repeat(' ', indent) + field.getName() + ":" + field.getType().getName() + (field.getType().isEnum() ? " (is enum)" : "") + "\n";
			if(!field.getType().isPrimitive()) {
				ret += getGenericTypeTree(field.getType(), indent + 4);
			}
		}
		return ret;
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private Utils() {
		throw new RuntimeException();
	}
}
