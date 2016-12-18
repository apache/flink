/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ${package};

import java.util.Iterator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

/**
 * Skeleton class showing how to write integration tests against a running embedded Flink
 * testing cluster.
 */
public class TestSkeleton {

	/**
	 * Test {@link SocketTextStreamWordCount} example job
	 */
	@Test
	public void testSocketTextStreamWordCount() throws Exception {
		final int ELEMENT_COUNT = 10_000;
		StreamExecutionEnvironment see = StreamExecutionEnvironment.createLocalEnvironment();

		// get input data
		DataStream<String> text = see.addSource(new NStringsGenerator(ELEMENT_COUNT));

		DataStream<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new SocketTextStreamWordCount.LineSplitter())
						// group by the tuple field "0" and sum up tuple field "1"
						.keyBy(0)
						.sum(1);


		Iterator<Tuple2<String, Integer>> elements = DataStreamUtils.collect(counts);
		int totalCount = 0;
		// ensure we see each of the 10 numbers 1000 times.
		int[] keyedCount = new int[10];
		while(elements.hasNext()) {
			Tuple2<String, Integer> element = elements.next();
			keyedCount[Integer.parseInt(element.f0)]++;
			totalCount++;
		}

		for(int k: keyedCount) {
			Assert.assertEquals(1000, k);
		}

		Assert.assertEquals(ELEMENT_COUNT, totalCount);
	}

	/**
	 * Generate N strings
	 */
	private static class NStringsGenerator implements SourceFunction<String> {
		private final int n;

		NStringsGenerator(int n) {
			this.n = n;
		}

		@Override
		public void run(SourceContext<String> sourceContext) throws Exception {
			int i = 0;
			while(i++ < n) {
				sourceContext.collect(Integer.toString(i % 10));
			}
		}

		@Override
		public void cancel() {

		}
	}

	// ---------------------------------------------------------


	/**
	 * This test shows how to start an infinitely running streaming job.
	 *
	 * With the SuccessException() / tryExecute() method, we can stop the streaming job from within
	 * user defined functions.
	 */
	@Test
	public void testInfiniteStream() throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.createLocalEnvironment();

		// create infinite stream
		DataStream<String> elements = see.addSource(new TestingSource(-1));
		elements.flatMap(new FlatMapFunction<String, String>() {
			private long count = 0;
			@Override
			public void flatMap(String s, Collector<String> collector) throws Exception {
				if(count++ == 10_000) {
					// intentionally fail infinite job by throwing a SuccessException.
					throw new SuccessException();
				}
			}
		});

		// filters SuccessException.
		TestUtils.tryExecute(see, "infinite stream");
	}

	/**
	 * This test shows how the TestUtils.tryExecute() method forwards runtime exceptions to the test.
	 */
	@Test(expected = AssertionError.class)
	public void testFailureStream() throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.createLocalEnvironment();

		// create infinite stream
		DataStream<String> elements = see.addSource(new TestingSource(-1));
		elements.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String s, Collector<String> collector) throws Exception {
				// for the infinite TestingSource, the count will be negative in the beginning
				Assert.assertTrue(Long.parseLong(s.split("-")[1]) > 0);
				// this will never be thrown
				throw new SuccessException();
			}
		});

		// filters SuccessException.
		TestUtils.tryExecute(see, "infinite failing stream");
	}


	/**
	 * Utility data source (non-parallel)
	 */
	private class TestingSource implements SourceFunction<String> {
		private long elements;
		private boolean running = true;

		TestingSource(long elements) {
			this.elements = elements;
		}

		@Override
		public void run(SourceContext<String> sourceContext) throws Exception {
			while(elements-- != 0 && running ) {
				sourceContext.collect("element-"+elements);
			}
		}

		@Override
		public void cancel() {
			this.running = false;
		}
	}


}
