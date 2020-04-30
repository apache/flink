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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

/** Test suite for {@link CompositeSerializer}. */
public class CompositeSerializerTest {
	private static final ExecutionConfig execConf = new ExecutionConfig();

	private static final List<Tuple2<TypeSerializer<?>, Object[]>> TEST_FIELD_SERIALIZERS = Arrays.asList(
		Tuple2.of(BooleanSerializer.INSTANCE, new Object[] { true, false }),
		Tuple2.of(LongSerializer.INSTANCE, new Object[] { 1L, 23L }),
		Tuple2.of(StringSerializer.INSTANCE, new Object[] { "teststr1", "teststr2" }),
		Tuple2.of(TypeInformation.of(Pojo.class).createSerializer(execConf),
			new Object[] { new Pojo(3, new String[] { "123", "456" }), new Pojo(6, new String[] {  }) })
	);

	@Test
	public void testSingleFieldSerializer() {
		TEST_FIELD_SERIALIZERS.forEach(t -> {
			@SuppressWarnings("unchecked")
			TypeSerializer<Object>[] fieldSerializers = new TypeSerializer[] { t.f0 };
			List<Object>[] instances = Arrays.stream(t.f1)
				.map(Arrays::asList)
				.toArray((IntFunction<List<Object>[]>) List[]::new);
			runTests(t.f0.getLength(), fieldSerializers, instances);
		});
	}

	@Test
	public void testPairFieldSerializer() {
		TEST_FIELD_SERIALIZERS.forEach(t1 ->
			TEST_FIELD_SERIALIZERS.forEach(t2 -> {
				@SuppressWarnings("unchecked")
				TypeSerializer<Object>[] fieldSerializers = new TypeSerializer[] { t1.f0, t2.f0 };
				List<Object>[] instances = IntStream.range(0, t1.f1.length)
					.mapToObj(i -> Arrays.asList(t1.f1[i], t2.f1[i]))
					.toArray((IntFunction<List<Object>[]>) List[]::new);
				runTests(getLength(fieldSerializers), fieldSerializers, instances);
			}));
	}

	@Test
	public void testAllFieldSerializer() {
		@SuppressWarnings("unchecked")
		TypeSerializer<Object>[] fieldSerializers = TEST_FIELD_SERIALIZERS.stream()
			.map(t -> (TypeSerializer<Object>) t.f0)
			.toArray((IntFunction<TypeSerializer<Object>[]>) TypeSerializer[]::new);
		List<Object>[] instances = IntStream.range(0, TEST_FIELD_SERIALIZERS.get(0).f1.length)
			.mapToObj(CompositeSerializerTest::getTestCase)
			.toArray((IntFunction<List<Object>[]>) List[]::new);
		runTests(getLength(fieldSerializers), fieldSerializers, instances);
	}

	// needs to be Arrays.ArrayList for all tests
	private static List<Object> getTestCase(int index) {
		return Arrays.asList(TEST_FIELD_SERIALIZERS.stream()
			.map(t -> t.f1[index])
			.toArray(Object[]::new));
	}

	private static int getLength(TypeSerializer<Object>[] fieldSerializers) {
		return Arrays.stream(fieldSerializers).allMatch(fs -> fs.getLength() > 0) ?
			Arrays.stream(fieldSerializers).mapToInt(TypeSerializer::getLength).sum() : -1;
	}

	@SuppressWarnings("unchecked")
	private void runTests(
		int length,
		TypeSerializer<Object>[] fieldSerializers,
		List<Object> ... instances) {
		try {
			for (boolean immutability : Arrays.asList(true, false)) {
				TypeSerializer<List<Object>> serializer = new TestListCompositeSerializer(immutability, fieldSerializers);
				CompositeSerializerTestInstance test = new CompositeSerializerTestInstance(serializer, length, instances);
				test.testAll();
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	private static class Pojo {
		public int f1;
		public String[] f2;

		private Pojo(int f1, String[] f2) {
			this.f1 = f1;
			this.f2 = f2;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Pojo pojo = (Pojo) o;
			return f1 == pojo.f1 &&
				Arrays.equals(f2, pojo.f2);
		}

		@Override
		public int hashCode() {

			int result = Objects.hash(f1);
			result = 31 * result + Arrays.hashCode(f2);
			return result;
		}

		@Override
		public String toString() {
			return "Pojo{" +
				"f1=" + f1 +
				", f2=" + Arrays.toString(f2) +
				'}';
		}
	}

	private static class TestListCompositeSerializer extends CompositeSerializer<List<Object>> {
		TestListCompositeSerializer(boolean isImmutableTargetType, TypeSerializer<?>... fieldSerializers) {
			super(isImmutableTargetType, fieldSerializers);
		}

		TestListCompositeSerializer(PrecomputedParameters precomputed, TypeSerializer<?>... fieldSerializers) {
			super(precomputed, fieldSerializers);
		}

		@Override
		public List<Object> createInstance(@Nonnull Object... values) {
			return Arrays.asList(values);
		}

		@Override
		protected void setField(@Nonnull List<Object> value, int index, Object fieldValue) {
			if (precomputed.immutable) {
				throw new UnsupportedOperationException("Type is immutable");
			} else {
				value.set(index, fieldValue);
			}
		}

		@Override
		protected Object getField(@Nonnull List<Object> value, int index) {
			return value.get(index);
		}

		@Override
		protected CompositeSerializer<List<Object>> createSerializerInstance(
			PrecomputedParameters precomputed, TypeSerializer<?>... originalSerializers) {
			return new TestListCompositeSerializer(precomputed, originalSerializers);
		}

		@Override
		public TypeSerializerSnapshot<List<Object>> snapshotConfiguration() {
			return new TestListCompositeSerializerSnapshot(this, precomputed.immutableTargetType);
		}
	}

	public static class TestListCompositeSerializerSnapshot
		extends CompositeTypeSerializerSnapshot<List<Object>, TestListCompositeSerializer> {

		private boolean isImmutableTargetType;

		/**
		 * Constructor for read instantiation.
		 */
		public TestListCompositeSerializerSnapshot() {
			super(TestListCompositeSerializer.class);
			this.isImmutableTargetType = false;
		}

		/**
		 * Constructor to create the snapshot for writing.
		 */
		public TestListCompositeSerializerSnapshot(
				TestListCompositeSerializer serializerInstance,
				boolean isImmutableTargetType) {
			super(serializerInstance);
			this.isImmutableTargetType = isImmutableTargetType;
		}

		@Override
		protected int getCurrentOuterSnapshotVersion() {
			return 0;
		}

		@Override
		protected void writeOuterSnapshot(DataOutputView out) throws IOException {
			out.writeBoolean(isImmutableTargetType);
		}

		@Override
		protected void readOuterSnapshot(
				int readOuterSnapshotVersion,
				DataInputView in,
				ClassLoader userCodeClassLoader) throws IOException {
			this.isImmutableTargetType = in.readBoolean();
		}

		@Override
		protected TypeSerializer<?>[] getNestedSerializers(TestListCompositeSerializer outerSerializer) {
			return outerSerializer.fieldSerializers;
		}

		@Override
		protected TestListCompositeSerializer createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
			return new TestListCompositeSerializer(isImmutableTargetType, nestedSerializers);
		}
	}

	private static class CompositeSerializerTestInstance extends SerializerTestInstance<List<Object>> {
		@SuppressWarnings("unchecked")
		CompositeSerializerTestInstance(
			TypeSerializer<List<Object>> serializer,
			int length,
			List<Object> ... testData) {
			super(serializer, getCls(testData[0]), length, testData);
		}

		private static Class<List<Object>> getCls(List<Object> instance) {
			return TypeExtractor.getForObject(instance).getTypeClass();
		}
	}
}
