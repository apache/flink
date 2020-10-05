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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.SplittableIterator;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * An end to end test for sorted inputs for a keyed operator with bounded inputs.
 */
public class SortingBoundedInputITCase {
	@Test
	public void testOneInputOperator() throws Exception {
		long numberOfRecords = 1_000_000;
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Tuple2<Integer, byte[]>> elements = env.fromParallelCollection(
			new InputGenerator(numberOfRecords),
			new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
		);

		SingleOutputStreamOperator<Long> counts = elements
			.keyBy(element -> element.f0)
			.transform(
				"Asserting operator",
				BasicTypeInfo.LONG_TYPE_INFO,
				new AssertingOperator()
			);

		// TODO we should replace this block with DataStreamUtils#collect once
		// we have the automatic runtime mode determination in place.
		CollectResultIterator<Long> collectedCounts = applyCollect(env, counts);
		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.getStreamNode(counts.getId()).setSortedInputs(true);
		HashMap<ManagedMemoryUseCase, Integer> operatorMemory = new HashMap<>();
		operatorMemory.put(ManagedMemoryUseCase.BATCH_OP, 1);
		streamGraph.getStreamNode(counts.getId()).setManagedMemoryUseCaseWeights(
			operatorMemory,
			Collections.emptySet()
		);
		JobClient jobClient = env.executeAsync(streamGraph);
		collectedCounts.setJobClient(jobClient);

		long sum = CollectionUtil.iteratorToList(collectedCounts)
			.stream()
			.mapToLong(l -> l)
			.sum();

		assertThat(numberOfRecords, equalTo(sum));
	}

	private CollectResultIterator<Long> applyCollect(StreamExecutionEnvironment env, SingleOutputStreamOperator<Long> counts) {
		String accumulatorName = "dataStreamCollect_" + UUID.randomUUID().toString();

		CollectSinkOperatorFactory<Long> factory = new CollectSinkOperatorFactory<>(
			new LongSerializer(),
			accumulatorName);
		CollectSinkOperator<Long> operator = (CollectSinkOperator<Long>) factory.getOperator();
		CollectStreamSink<Long> sink = new CollectStreamSink<>(counts, factory);
		sink.name("Data stream collect sink");
		env.addOperator(sink.getTransformation());

		return new CollectResultIterator<>(
			operator.getOperatorIdFuture(),
			new LongSerializer(),
			accumulatorName,
			env.getCheckpointConfig());
	}

	private static class AssertingOperator extends AbstractStreamOperator<Long>
			implements OneInputStreamOperator<Tuple2<Integer, byte[]>, Long>, BoundedOneInput {
		private final Set<Integer> seenKeys = new HashSet<>();
		private long seenRecords = 0;
		private Integer currentKey = null;

		@Override
		public void processElement(StreamRecord<Tuple2<Integer, byte[]>> element) throws Exception {
			this.seenRecords++;
			Integer incomingKey = element.getValue().f0;
			if (!Objects.equals(incomingKey, currentKey)) {
				if (!seenKeys.add(incomingKey)) {
					Assert.fail("Received an out of order key: " + incomingKey);
				}
				this.currentKey = incomingKey;
			}
		}

		@Override
		public void endInput() throws Exception {
			output.collect(new StreamRecord<>(seenRecords));
		}
	}

	private static class InputGenerator extends SplittableIterator<Tuple2<Integer, byte[]>> {

		private final long numberOfRecords;
		private long generatedRecords;
		private final Random rnd = new Random();
		private final byte[] bytes = new byte[500];

		private InputGenerator(long numberOfRecords) {
			this.numberOfRecords = numberOfRecords;
			rnd.nextBytes(bytes);
		}

		@Override
		@SuppressWarnings("unchecked")
		public Iterator<Tuple2<Integer, byte[]>>[] split(int numPartitions) {
			long numberOfRecordsPerPartition = numberOfRecords / numPartitions;
			long remainder = numberOfRecords % numPartitions;
			Iterator<Tuple2<Integer, byte[]>>[] iterators = new Iterator[numPartitions];

			for (int i = 0; i < numPartitions - 1; i++) {
				iterators[i] = new InputGenerator(numberOfRecordsPerPartition);
			}

			iterators[numPartitions - 1] = new InputGenerator(numberOfRecordsPerPartition + remainder);

			return iterators;
		}

		@Override
		public int getMaximumNumberOfSplits() {
			return (int) Math.min(numberOfRecords, Integer.MAX_VALUE);
		}

		@Override
		public boolean hasNext() {
			return generatedRecords < numberOfRecords;
		}

		@Override
		public Tuple2<Integer, byte[]> next() {
			if (hasNext()) {
				generatedRecords++;
				return Tuple2.of(
					rnd.nextInt(10),
					bytes
				);
			}

			return null;
		}
	}
}
