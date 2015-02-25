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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;

import java.util.List;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


public class Utils {

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
	 * @return
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

	public static class CollectHelper<T> extends RichFlatMapFunction<T, T> {

		private static final long serialVersionUID = 1L;

		private final String id;
		private final ListAccumulator<T> accumulator;

		public CollectHelper(String id) {
			this.id = id;
			this.accumulator = new ListAccumulator<T>();
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator(id, accumulator);
		}

		@Override
		public void flatMap(T value, Collector<T> out) throws Exception {
			accumulator.add(value);
		}
	}

}
