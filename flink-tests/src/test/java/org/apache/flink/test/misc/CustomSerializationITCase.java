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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.types.Value;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("serial")
public class CustomSerializationITCase {

	private static final int PARLLELISM = 5;
	
	private static ForkableFlinkMiniCluster cluster;

	@BeforeClass
	public static void startCluster() {
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, PARLLELISM);
			config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 30);
			cluster = new ForkableFlinkMiniCluster(config, false);
			cluster.start();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Failed to start test cluster: " + e.getMessage());
		}
	}

	@AfterClass
	public static void shutdownCluster() {
		try {
			cluster.shutdown();
			cluster = null;
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Failed to stop test cluster: " + e.getMessage());
		}
	}
	
	@Test
	public void testIncorrectSerializer1() {
		try {
			ExecutionEnvironment env =
					ExecutionEnvironment.createRemoteEnvironment("localhost", cluster.getLeaderRPCPort());
			
			env.setParallelism(PARLLELISM);
			env.getConfig().disableSysoutLogging();
			
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

	@Test
	public void testIncorrectSerializer2() {
		try {
			ExecutionEnvironment env =
					ExecutionEnvironment.createRemoteEnvironment("localhost", cluster.getLeaderRPCPort());

			env.setParallelism(PARLLELISM);
			env.getConfig().disableSysoutLogging();

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

	@Test
	public void testIncorrectSerializer3() {
		try {
			ExecutionEnvironment env =
					ExecutionEnvironment.createRemoteEnvironment("localhost", cluster.getLeaderRPCPort());

			env.setParallelism(PARLLELISM);
			env.getConfig().disableSysoutLogging();

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

	@Test
	public void testIncorrectSerializer4() {
		try {
			ExecutionEnvironment env =
					ExecutionEnvironment.createRemoteEnvironment("localhost", cluster.getLeaderRPCPort());

			env.setParallelism(PARLLELISM);
			env.getConfig().disableSysoutLogging();

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
