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
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Value;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for proper error messages in case user-defined serialization is broken
 * and detected in the network stack.
 */
@SuppressWarnings("serial")
public class CustomSerializationITCase extends TestLogger {

	private static final int PARLLELISM = 5;

	@ClassRule
	public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE = new MiniClusterWithClientResource(
		new MiniClusterResourceConfiguration.Builder()
			.setConfiguration(getConfiguration())
			.setNumberTaskManagers(1)
			.setNumberSlotsPerTaskManager(PARLLELISM)
			.build());

	public static Configuration getConfiguration() {
		Configuration config = new Configuration();
		config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("30m"));
		return config;
	}

	@Test
	public void testIncorrectSerializer1() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARLLELISM);

			env
				.generateSequence(1, 10 * PARLLELISM)
				.map(new MapFunction<Long, ConsumesTooMuch>() {
					@Override
					public ConsumesTooMuch map(Long value) throws Exception {
						return new ConsumesTooMuch();
					}
				})
				.rebalance()
				.output(new DiscardingOutputFormat<ConsumesTooMuch>());

			env.execute();
		}
		catch (JobExecutionException e) {
			Optional<IOException> rootCause = findThrowable(e, IOException.class);
			assertTrue(rootCause.isPresent());
			assertTrue(rootCause.get().getMessage().contains("broken serialization"));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testIncorrectSerializer2() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARLLELISM);

			env
					.generateSequence(1, 10 * PARLLELISM)
					.map(new MapFunction<Long, ConsumesTooMuchSpanning>() {
						@Override
						public ConsumesTooMuchSpanning map(Long value) throws Exception {
							return new ConsumesTooMuchSpanning();
						}
					})
					.rebalance()
					.output(new DiscardingOutputFormat<ConsumesTooMuchSpanning>());

			env.execute();
		}
		catch (JobExecutionException e) {
			Optional<IOException> rootCause = findThrowable(e, IOException.class);
			assertTrue(rootCause.isPresent());
			assertTrue(rootCause.get().getMessage().contains("broken serialization"));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testIncorrectSerializer3() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARLLELISM);

			env
					.generateSequence(1, 10 * PARLLELISM)
					.map(new MapFunction<Long, ConsumesTooLittle>() {
						@Override
						public ConsumesTooLittle map(Long value) throws Exception {
							return new ConsumesTooLittle();
						}
					})
					.rebalance()
					.output(new DiscardingOutputFormat<ConsumesTooLittle>());

			env.execute();
		}
		catch (JobExecutionException e) {
			Optional<IOException> rootCause = findThrowable(e, IOException.class);
			assertTrue(rootCause.isPresent());
			assertTrue(rootCause.get().getMessage().contains("broken serialization"));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testIncorrectSerializer4() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARLLELISM);

			env
					.generateSequence(1, 10 * PARLLELISM)
					.map(new MapFunction<Long, ConsumesTooLittleSpanning>() {
						@Override
						public ConsumesTooLittleSpanning map(Long value) throws Exception {
							return new ConsumesTooLittleSpanning();
						}
					})
					.rebalance()
					.output(new DiscardingOutputFormat<ConsumesTooLittleSpanning>());

			env.execute();
		}
		catch (ProgramInvocationException e) {
			Throwable rootCause = e.getCause().getCause();
			assertTrue(rootCause instanceof IOException);
			assertTrue(rootCause.getMessage().contains("broken serialization"));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
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
