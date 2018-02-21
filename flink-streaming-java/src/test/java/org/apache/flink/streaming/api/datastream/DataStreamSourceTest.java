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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link DataStreamSource}.
 */
public class DataStreamSourceTest {

	private StreamExecutionEnvironment env;

	private DataStreamSource<String> dataStreamSource;

	@Before
	public void setUp() {
		env = StreamExecutionEnvironment.getExecutionEnvironment();

		dataStreamSource = env.addSource(new NoOpSourceFunction());
		dataStreamSource.name(UUID.randomUUID().toString());

		dataStreamSource.print();
	}

	@Test
	public void testSetParallelismOneIfSourceIsNonParallel() {
		final JobVertex testSourceVertex = StreamSupport.stream(env
				.getStreamGraph()
				.getJobGraph()
				.getVertices()
				.spliterator(),
			false)
			.filter(jobVertex -> jobVertex.getName().contains(dataStreamSource.getName()))
			.findFirst()
			.orElseThrow(() -> new AssertionError("testSource vertex not found"));

		assertThat(testSourceVertex.getMaxParallelism(), equalTo(1));
		assertThat(testSourceVertex.getParallelism(), equalTo(1));
	}

	@Test
	public void testCannotSetMaxParallelism() {
		try {
			dataStreamSource.setMaxParallelism(2);
			fail("Setting maxParallelism should fail.");
		} catch (final IllegalArgumentException e) {
			assertThat(e.getMessage(), containsString("The maximum parallelism of non parallel operator must be 1."));
		}
	}

	@Test
	public void testCannotSetParallelism() {
		try {
			dataStreamSource.setParallelism(2);
			fail("Setting parallelism should fail.");
		} catch (final IllegalArgumentException e) {
			assertThat(e.getMessage(), containsString("The parallelism of non parallel operator must be 1."));
		}
	}

	private static class NoOpSourceFunction implements SourceFunction<String> {

		private static final long serialVersionUID = 1L;

		@Override
		public void run(final SourceContext<String> ctx) throws Exception {
		}

		@Override
		public void cancel() {
		}
	}
}
