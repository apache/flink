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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link VariableLengthByteKeyComparator}.
 */
public class VariableLengthByteKeyComparatorTest extends ComparatorTestBase<Tuple2<byte[], StreamRecord<String>>> {
	@Override
	protected boolean[] getTestedOrder() {
		return new boolean[]{true};
	}

	@Override
	protected TypeComparator<Tuple2<byte[], StreamRecord<String>>> createComparator(boolean ascending) {
		return new VariableLengthByteKeyComparator<>();
	}

	@Override
	protected TypeSerializer<Tuple2<byte[], StreamRecord<String>>> createSerializer() {
		StringSerializer stringSerializer = new StringSerializer();
		return new KeyAndValueSerializer<>(
			stringSerializer,
			stringSerializer.getLength()
		);
	}

	@Override
	protected void deepEquals(
			String message,
			Tuple2<byte[], StreamRecord<String>> should,
			Tuple2<byte[], StreamRecord<String>> is) {
		assertThat(message, should.f0, equalTo(is.f0));
		assertThat(message, should.f1, equalTo(is.f1));
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Tuple2<byte[], StreamRecord<String>>[] getSortedTestData() {
		StringSerializer stringSerializer = new StringSerializer();
		DataOutputSerializer outputSerializer = new DataOutputSerializer(64);
		return Stream.of(
			new String(new byte[] {-1, 0}),
			new String(new byte[] {0, 1}),
			"A",
			"AB",
			"ABC",
			"ABCD",
			"ABCDE",
			"ABCDEF",
			"ABCDEFG",
			"ABCDEFGH")
			.map(
				str -> {
					try {
						stringSerializer.serialize(str, outputSerializer);
						byte[] copyOfBuffer = outputSerializer.getCopyOfBuffer();
						outputSerializer.clear();
						return Tuple2.of(copyOfBuffer, new StreamRecord<>(str, 0));
					} catch (IOException e) {
						throw new AssertionError(e);
					}
				}
			).sorted(
			(o1, o2) -> {
				byte[] key0 = o1.f0;
				byte[] key1 = o2.f0;

				int firstLength = key0.length;
				int secondLength = key1.length;
				int minLength = Math.min(firstLength, secondLength);
				for (int i = 0; i < minLength; i++) {
					int cmp = Byte.compare(key0[i], key1[i]);

					if (cmp != 0) {
						return cmp;
					}
				}

				int lengthCmp = Integer.compare(firstLength, secondLength);
				if (lengthCmp != 0) {
					return lengthCmp;
				}
				return Long.compare(o1.f1.getTimestamp(), o2.f1.getTimestamp());
			}
		).toArray(Tuple2[]::new);
	}
}
