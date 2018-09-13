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

package org.apache.flink.test.misc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Value;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.Matchers.hasProperty;

/**
 * Test for proper error messages in case user-defined serialization is broken
 * and detected in the network stack.
 */
@SuppressWarnings("serial")
public class CustomSerializationITCase extends TestLogger {

	private static final int PARLLELISM = 5;

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@ClassRule
	public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE = new MiniClusterWithClientResource(
		new MiniClusterResourceConfiguration.Builder()
			.setConfiguration(getConfiguration())
			.setNumberTaskManagers(1)
			.setNumberSlotsPerTaskManager(PARLLELISM)
			.build());

	public static Configuration getConfiguration() {
		Configuration config = new Configuration();
		config.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "30m");
		return config;
	}

	@Test
	public void testIncorrectSerializer1() throws Exception {
		testIncorrectSerializer(value -> new ConsumesTooMuch());
	}

	@Test
	public void testIncorrectSerializer2() throws Exception {
		testIncorrectSerializer(value -> new ConsumesTooMuchSpanning());
	}

	@Test
	public void testIncorrectSerializer3() throws Exception {
		testIncorrectSerializer(value -> new ConsumesTooLittle());
	}

	@Test
	public void testIncorrectSerializer4() throws Exception {
		testIncorrectSerializer(value -> new ConsumesTooLittleSpanning());
	}

	private <T> void testIncorrectSerializer(MapFunction<Long, T> mapper) throws Exception {
		expectedException.expect(JobExecutionException.class);
		expectedException.expectCause(
			Matchers.both(isA(IOException.class))
				.and(hasProperty("message", containsString("broken serialization"))));

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(PARLLELISM);
		env.getConfig().disableSysoutLogging();

		env
			.generateSequence(1, 10 * PARLLELISM)
			.map(mapper)
			.rebalance()
			.output(new DiscardingOutputFormat<>());

		env.execute();
	}

	// ------------------------------------------------------------------------
	//  Custom Data Types with broken Serialization Logic
	// ------------------------------------------------------------------------

	/**
	 * {@link Value} reading more data than written.
	 */
	public static class ConsumesTooMuch implements Value {

		@Override
		public void write(DataOutputView out) throws IOException {
			// write 4 bytes
			out.writeInt(42);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			// read 8 bytes
			in.readLong();
		}
	}

	/**
	 * {@link Value} reading more buffers than written.
	 */
	public static class ConsumesTooMuchSpanning implements Value {

		@Override
		public void write(DataOutputView out) throws IOException {
			byte[] bytes = new byte[22541];
			out.write(bytes);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			byte[] bytes = new byte[32941];
			in.readFully(bytes);
		}
	}

	/**
	 * {@link Value} reading less data than written.
	 */
	public static class ConsumesTooLittle implements Value {

		@Override
		public void write(DataOutputView out) throws IOException {
			// write 8 bytes
			out.writeLong(42L);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			// read 4 bytes
			in.readInt();
		}
	}

	/**
	 * {@link Value} reading fewer buffers than written.
	 */
	public static class ConsumesTooLittleSpanning implements Value {

		@Override
		public void write(DataOutputView out) throws IOException {
			byte[] bytes = new byte[32941];
			out.write(bytes);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			byte[] bytes = new byte[22541];
			in.readFully(bytes);
		}
	}
}
