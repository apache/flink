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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Optional;
import java.util.Random;

/**
 * Utility class that contains helper methods to work with Java APIs.
 */
@Internal
public final class Utils {

	public static final Random RNG = new Random();

	public static String getCallLocationName() {
		return getCallLocationName(4);
	}

	public static String getCallLocationName(int depth) {
		StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

		if (stackTrace.length <= depth) {
			return "<unknown>";
		}

		StackTraceElement elem = stackTrace[depth];

		return String.format("%s(%s:%d)", elem.getMethodName(), elem.getFileName(), elem.getLineNumber());
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Utility sink function that counts elements and writes the count into an accumulator,
	 * from which it can be retrieved by the client. This sink is used by the
	 * {@link DataSet#count()} function.
	 *
	 * @param <T> Type of elements to count.
	 */
	public static class CountHelper<T> extends RichOutputFormat<T> {

		private static final long serialVersionUID = 1L;

		private final String id;
		private long counter;

		public CountHelper(String id) {
			this.id = id;
			this.counter = 0L;
		}

		@Override
		public void configure(Configuration parameters) {}

		@Override
		public void open(int taskNumber, int numTasks) {}

		@Override
		public void writeRecord(T record) {
			counter++;
		}

		@Override
		public void close() {
			getRuntimeContext().getLongCounter(id).add(counter);
		}
	}

	/**
	 * Utility sink function that collects elements into an accumulator,
	 * from which it they can be retrieved by the client. This sink is used by the
	 * {@link DataSet#collect()} function.
	 *
	 * @param <T> Type of elements to count.
	 */
	public static class CollectHelper<T> extends RichOutputFormat<T> {

		private static final long serialVersionUID = 1L;

		private final String id;
		private final TypeSerializer<T> serializer;

		private SerializedListAccumulator<T> accumulator;

		public CollectHelper(String id, TypeSerializer<T> serializer) {
			this.id = id;
			this.serializer = serializer;
		}

		@Override
		public void configure(Configuration parameters) {}

		@Override
		public void open(int taskNumber, int numTasks)  {
			this.accumulator = new SerializedListAccumulator<>();
		}

		@Override
		public void writeRecord(T record) throws IOException {
			accumulator.add(record, serializer);
		}

		@Override
		public void close() {
			// Important: should only be added in close method to minimize traffic of accumulators
			getRuntimeContext().addAccumulator(id, accumulator);
		}
	}

	/**
	 * Accumulator of {@link ChecksumHashCode}.
	 */
	public static class ChecksumHashCode implements SimpleAccumulator<ChecksumHashCode> {

		private static final long serialVersionUID = 1L;

		private long count;
		private long checksum;

		public ChecksumHashCode() {}

		public ChecksumHashCode(long count, long checksum) {
			this.count = count;
			this.checksum = checksum;
		}

		public long getCount() {
			return count;
		}

		public long getChecksum() {
			return checksum;
		}

		@Override
		public void add(ChecksumHashCode value) {
			this.count += value.count;
			this.checksum += value.checksum;
		}

		@Override
		public ChecksumHashCode getLocalValue() {
			return this;
		}

		@Override
		public void resetLocal() {
			this.count = 0;
			this.checksum = 0;
		}

		@Override
		public void merge(Accumulator<ChecksumHashCode, ChecksumHashCode> other) {
			this.add(other.getLocalValue());
		}

		@Override
		public ChecksumHashCode clone() {
			return new ChecksumHashCode(count, checksum);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ChecksumHashCode) {
				ChecksumHashCode other = (ChecksumHashCode) obj;
				return this.count == other.count && this.checksum == other.checksum;
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return (int) (this.count + this.checksum);
		}

		@Override
		public String toString() {
			return String.format("ChecksumHashCode 0x%016x, count %d", this.checksum, this.count);
		}
	}

	/**
	 * {@link RichOutputFormat} for {@link ChecksumHashCode}.
	 * @param <T>
	 */
	public static class ChecksumHashCodeHelper<T> extends RichOutputFormat<T> {

		private static final long serialVersionUID = 1L;

		private final String id;
		private long counter;
		private long checksum;

		public ChecksumHashCodeHelper(String id) {
			this.id = id;
			this.counter = 0L;
			this.checksum = 0L;
		}

		@Override
		public void configure(Configuration parameters) {}

		@Override
		public void open(int taskNumber, int numTasks) {}

		@Override
		public void writeRecord(T record) throws IOException {
			counter++;
			// convert 32-bit integer to non-negative long
			checksum += record.hashCode() & 0xffffffffL;
		}

		@Override
		public void close() throws IOException {
			ChecksumHashCode update = new ChecksumHashCode(counter, checksum);
			getRuntimeContext().addAccumulator(id, update);
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
		if (ti instanceof CompositeType) {
			ret += StringUtils.repeat(' ', indent) + ti.getClass().getSimpleName() + "\n";
			CompositeType<T> cti = (CompositeType<T>) ti;
			String[] fieldNames = cti.getFieldNames();
			for (int i = 0; i < cti.getArity(); i++) {
				TypeInformation<?> fieldType = cti.getTypeAt(i);
				ret += StringUtils.repeat(' ', indent + 2) + fieldNames[i] + ":" + getSerializerTree(fieldType, indent);
			}
		} else {
			if (ti instanceof GenericTypeInfo) {
				ret += StringUtils.repeat(' ', indent) + "GenericTypeInfo (" + ti.getTypeClass().getSimpleName() + ")\n";
				ret += getGenericTypeTree(ti.getTypeClass(), indent + 4);
			} else {
				ret += StringUtils.repeat(' ', indent) + ti.toString() + "\n";
			}
		}
		return ret;
	}

	private static String getGenericTypeTree(Class<?> type, int indent) {
		String ret = "";
		for (Field field : type.getDeclaredFields()) {
			if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) {
				continue;
			}
			ret += StringUtils.repeat(' ', indent) + field.getName() + ":" + field.getType().getName() +
				(field.getType().isEnum() ? " (is enum)" : "") + "\n";
			if (!field.getType().isPrimitive()) {
				ret += getGenericTypeTree(field.getType(), indent + 4);
			}
		}
		return ret;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Resolves the given factories. The thread local factory has preference over the static factory.
	 * If none is set, the method returns {@link Optional#empty()}.
	 *
	 * @param threadLocalFactory containing the thread local factory
	 * @param staticFactory containing the global factory
	 * @param <T> type of factory
	 * @return Optional containing the resolved factory if it exists, otherwise it's empty
	 */
	public static <T> Optional<T> resolveFactory(ThreadLocal<T> threadLocalFactory, @Nullable T staticFactory) {
		final T localFactory = threadLocalFactory.get();
		final T factory = localFactory == null ? staticFactory : localFactory;

		return Optional.ofNullable(factory);
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private Utils() {
		throw new RuntimeException();
	}
}
