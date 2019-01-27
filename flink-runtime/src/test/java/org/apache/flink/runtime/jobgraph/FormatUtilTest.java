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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.jobgraph.FormatUtil.AbstractFormatStub;
import org.apache.flink.runtime.jobgraph.FormatUtil.FormatType;
import org.apache.flink.runtime.jobgraph.FormatUtil.InputFormatStub;
import org.apache.flink.runtime.jobgraph.FormatUtil.MultiFormatStub;
import org.apache.flink.runtime.jobgraph.FormatUtil.OutputFormatStub;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.types.Pair;

import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link FormatUtil}.
 */
public class FormatUtilTest {

	@Test
	public void testFormatStub() {
		try {
			new TestFormatStub(null, getClass().getClassLoader());
			fail();
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new TestFormatStub(new TaskConfig(new Configuration()), null);
			fail();
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new TestFormatStub(new TaskConfig(new Configuration()), getClass().getClassLoader());
			fail();
		} catch (Exception e) {
			assertEquals("Deserializing TestFormatStub failed: No TestFormatStub present in task configuration.", e.getMessage());
		}
	}

	@Test
	public void testInputFormatStub() throws Exception {
		OperatorID sourceOperatorId = new OperatorID(5L, 2L);
		OperatorID anotherOperatorId = new OperatorID(3L, 8L);

		// sub case:
		{
			TaskConfig taskConfig = new TaskConfig(new Configuration());
			taskConfig.setStubWrapper(new UserCodeObjectWrapper<>(new TestInputFormat("test input format stub")));
			taskConfig.setStubParameter("abc", "123");

			InputFormatStub stub = new InputFormatStub(taskConfig, getClass().getClassLoader(), sourceOperatorId);
			assertFalse(stub.getFormat(FormatType.OUTPUT).hasNext());

			Iterator<Pair<OperatorID, InputFormat>> it = stub.getFormat(FormatType.INPUT);
			assertTrue(it.hasNext());
			assertEquals("test input format stub", ((TestInputFormat) it.next().getValue()).getName());
			assertFalse(it.hasNext());
			assertTrue(stub.getFormat(FormatType.INPUT).hasNext());

			assertEquals(0, stub.getParameters(anotherOperatorId).keySet().size());

			Configuration parameters = stub.getParameters(sourceOperatorId);
			assertEquals(1, parameters.keySet().size());
			assertEquals("123", parameters.getString("abc", null));
		}

		// sub case:
		{
			TaskConfig taskConfig = new TaskConfig(new Configuration());
			InputFormatStub.setStubFormat(taskConfig, new TestInputFormat("test input format stub"));
			Configuration param = new Configuration();
			param.setString("abc", "123");
			InputFormatStub.setStubParameters(taskConfig, param);

			InputFormatStub stub = new InputFormatStub(taskConfig, getClass().getClassLoader(), sourceOperatorId);
			assertFalse(stub.getFormat(FormatType.OUTPUT).hasNext());

			Iterator<Pair<OperatorID, InputFormat>> it = stub.getFormat(FormatType.INPUT);
			assertTrue(it.hasNext());
			assertEquals("test input format stub", ((TestInputFormat) it.next().getValue()).getName());
			assertFalse(it.hasNext());
			assertTrue(stub.getFormat(FormatType.INPUT).hasNext());

			assertEquals(0, stub.getParameters(anotherOperatorId).keySet().size());

			Configuration parameters = stub.getParameters(sourceOperatorId);
			assertEquals(1, parameters.keySet().size());
			assertEquals("123", parameters.getString("abc", null));
		}

		// sub case:
		{
			TaskConfig taskConfig = new TaskConfig(new Configuration());
			InputFormatStub.setStubFormat(taskConfig, new TestInputFormat("test input format stub"));

			InputFormatStub stub = new InputFormatStub(taskConfig, getClass().getClassLoader(), sourceOperatorId);

			Configuration parameters = stub.getParameters(sourceOperatorId);
			assertEquals(0, parameters.keySet().size());
		}
	}

	@Test
	public void testOutputFormatStub() throws Exception {
		OperatorID sinkOperatorId = new OperatorID(7L, 0L);
		OperatorID anotherOperatorId = new OperatorID(9L, 1L);

		// sub case:
		{
			TaskConfig taskConfig = new TaskConfig(new Configuration());
			taskConfig.setStubParameter("abc", "456");
			taskConfig.setStubWrapper(new UserCodeObjectWrapper<>(new TestOutputFormat("test output format stub")));

			OutputFormatStub stub = new OutputFormatStub(taskConfig, getClass().getClassLoader(), sinkOperatorId);
			assertFalse(stub.getFormat(FormatType.INPUT).hasNext());

			Iterator<Pair<OperatorID, OutputFormat>> it = stub.getFormat(FormatType.OUTPUT);
			assertTrue(it.hasNext());
			assertEquals("test output format stub", ((TestOutputFormat) it.next().getValue()).getName());
			assertFalse(it.hasNext());
			assertTrue(stub.getFormat(FormatType.OUTPUT).hasNext());

			assertEquals(0, stub.getParameters(anotherOperatorId).keySet().size());

			Configuration parameters = stub.getParameters(sinkOperatorId);
			assertEquals(1, parameters.keySet().size());
			assertEquals("456", parameters.getString("abc", null));
		}

		// sub case:
		{
			TaskConfig taskConfig = new TaskConfig(new Configuration());
			OutputFormatStub.setStubFormat(taskConfig, new TestOutputFormat("test output format stub"));
			Configuration param = new Configuration();
			param.setString("abc", "456");
			OutputFormatStub.setStubParameters(taskConfig, param);

			OutputFormatStub stub = new OutputFormatStub(taskConfig, getClass().getClassLoader(), sinkOperatorId);
			assertFalse(stub.getFormat(FormatType.INPUT).hasNext());

			Iterator<Pair<OperatorID, OutputFormat>> it = stub.getFormat(FormatType.OUTPUT);
			assertTrue(it.hasNext());
			assertEquals("test output format stub", ((TestOutputFormat) it.next().getValue()).getName());
			assertFalse(it.hasNext());
			assertTrue(stub.getFormat(FormatType.OUTPUT).hasNext());

			assertEquals(0, stub.getParameters(anotherOperatorId).keySet().size());

			Configuration parameters = stub.getParameters(sinkOperatorId);
			assertEquals(1, parameters.keySet().size());
			assertEquals("456", parameters.getString("abc", null));
		}

		// sub case:
		{
			TaskConfig taskConfig = new TaskConfig(new Configuration());
			OutputFormatStub.setStubFormat(taskConfig, new TestOutputFormat("test output format stub"));

			InputFormatStub stub = new InputFormatStub(taskConfig, getClass().getClassLoader(), sinkOperatorId);

			Configuration parameters = stub.getParameters(sinkOperatorId);
			assertEquals(0, parameters.keySet().size());
		}
	}

	@Test
	public void testMultiFormatStub() throws Exception {
		// sub case:
		{
			TaskConfig taskConfig = new TaskConfig(new Configuration());

			OperatorID inputOperatorId1 = new OperatorID(123L, 0L);
			OperatorID inputOperatorId2 = new OperatorID(456L, 987L);
			OperatorID outputOperatorId1 = new OperatorID(222L, 3L);
			OperatorID outputOperatorId2 = new OperatorID(777L, 678L);

			{
				Map<OperatorID, InputFormat> inputFormatMap = new HashMap<>();
				inputFormatMap.put(inputOperatorId1, new TestInputFormat("testInputFormat1"));
				inputFormatMap.put(inputOperatorId2, new TestInputFormat("testInputFormat2"));

				Map<OperatorID, OutputFormat> outputFormatMap = new HashMap<>();
				outputFormatMap.put(outputOperatorId1, new TestOutputFormat("testOutputFormat1"));
				outputFormatMap.put(outputOperatorId2, new TestOutputFormat("testOutputFormat2"));

				MultiFormatStub.setStubFormats(taskConfig, inputFormatMap, outputFormatMap);

				Configuration inputFormatParams = new Configuration();
				inputFormatParams.setString("testInputFormat1.params", "123");
				MultiFormatStub.setStubParameters(taskConfig, inputOperatorId1, inputFormatParams);

				Configuration outputFormatParams = new Configuration();
				outputFormatParams.setString("testOutputFormat2.params", "456");
				MultiFormatStub.setStubParameters(taskConfig, outputOperatorId2, outputFormatParams);
			}

			MultiFormatStub stub = new MultiFormatStub(taskConfig, getClass().getClassLoader());
			{
				Map<OperatorID, TestInputFormat> inputFormatResultMap = new HashMap<>();
				Iterator<? extends Pair<OperatorID, ?>> it = stub.getFormat(FormatType.INPUT);
				while (it.hasNext()) {
					Pair<OperatorID, ?> pair = it.next();
					inputFormatResultMap.put(pair.getKey(), (TestInputFormat) pair.getValue());
				}
				assertEquals(2, inputFormatResultMap.size());
				assertEquals("testInputFormat1", inputFormatResultMap.get(inputOperatorId1).getName());
				assertEquals("testInputFormat2", inputFormatResultMap.get(inputOperatorId2).getName());

				Configuration inputFormat1ParamsResult = stub.getParameters(inputOperatorId1);
				assertEquals(1, inputFormat1ParamsResult.keySet().size());
				assertEquals("123", inputFormat1ParamsResult.getString("testInputFormat1.params", null));

				assertEquals(0, stub.getParameters(inputOperatorId2).keySet().size());
			}
			{
				Map<OperatorID, TestOutputFormat> outputFormatResultMap = new HashMap<>();
				Iterator<? extends Pair<OperatorID, ?>> it = stub.getFormat(FormatType.OUTPUT);
				while (it.hasNext()) {
					Pair<OperatorID, ?> pair = it.next();
					outputFormatResultMap.put(pair.getKey(), (TestOutputFormat) pair.getValue());
				}
				assertEquals(2, outputFormatResultMap.size());
				assertEquals("testOutputFormat1", outputFormatResultMap.get(outputOperatorId1).getName());
				assertEquals("testOutputFormat2", outputFormatResultMap.get(outputOperatorId2).getName());

				Configuration outputFormat2ParamsResult = stub.getParameters(outputOperatorId2);
				assertEquals(1, outputFormat2ParamsResult.keySet().size());
				assertEquals("456", outputFormat2ParamsResult.getString("testOutputFormat2.params", null));

				assertEquals(0, stub.getParameters(outputOperatorId1).keySet().size());
			}
		}

		// sub case:
		{
			TaskConfig taskConfig = new TaskConfig(new Configuration());

			OperatorID inputOperatorId1 = new OperatorID(543L, 21L);
			OperatorID inputOperatorId2 = new OperatorID(345L, 98L);

			{
				Map<OperatorID, InputFormat> inputFormatMap = new HashMap<>();
				inputFormatMap.put(inputOperatorId1, new TestInputFormat("testInputFormat1"));
				inputFormatMap.put(inputOperatorId2, new TestInputFormat("testInputFormat2"));

				MultiFormatStub.setStubFormats(taskConfig, inputFormatMap, null);

			}

			MultiFormatStub stub = new MultiFormatStub(taskConfig, getClass().getClassLoader());
			{
				Map<OperatorID, TestInputFormat> inputFormatResultMap = new HashMap<>();
				Iterator<? extends Pair<OperatorID, ?>> it = stub.getFormat(FormatType.INPUT);
				while (it.hasNext()) {
					Pair<OperatorID, ?> pair = it.next();
					inputFormatResultMap.put(pair.getKey(), (TestInputFormat) pair.getValue());
				}
				assertEquals(2, inputFormatResultMap.size());
				assertEquals("testInputFormat1", inputFormatResultMap.get(inputOperatorId1).getName());
				assertEquals("testInputFormat2", inputFormatResultMap.get(inputOperatorId2).getName());

				assertEquals(0, stub.getParameters(inputOperatorId1).keySet().size());
				assertEquals(0, stub.getParameters(inputOperatorId2).keySet().size());
			}

			assertFalse(stub.getFormat(FormatType.OUTPUT).hasNext());
		}

		// sub case:
		{
			TaskConfig taskConfig = new TaskConfig(new Configuration());

			OperatorID outputOperatorId1 = new OperatorID(65L, 23L);
			OperatorID outputOperatorId2 = new OperatorID(78L, 35L);

			{
				Map<OperatorID, OutputFormat> outputFormatMap = new HashMap<>();
				outputFormatMap.put(outputOperatorId1, new TestOutputFormat("testOutputFormat1"));
				outputFormatMap.put(outputOperatorId2, new TestOutputFormat("testOutputFormat2"));

				MultiFormatStub.setStubFormats(taskConfig, null, outputFormatMap);
			}

			MultiFormatStub stub = new MultiFormatStub(taskConfig, getClass().getClassLoader());
			{
				Map<OperatorID, TestOutputFormat> outputFormatResultMap = new HashMap<>();
				Iterator<? extends Pair<OperatorID, ?>> it = stub.getFormat(FormatType.OUTPUT);
				while (it.hasNext()) {
					Pair<OperatorID, ?> pair = it.next();
					outputFormatResultMap.put(pair.getKey(), (TestOutputFormat) pair.getValue());
				}
				assertEquals(2, outputFormatResultMap.size());
				assertEquals("testOutputFormat1", outputFormatResultMap.get(outputOperatorId1).getName());
				assertEquals("testOutputFormat2", outputFormatResultMap.get(outputOperatorId2).getName());

				assertEquals(0, stub.getParameters(outputOperatorId1).keySet().size());
				assertEquals(0, stub.getParameters(outputOperatorId2).keySet().size());
			}

			assertFalse(stub.getFormat(FormatType.INPUT).hasNext());
		}
	}

	// --------------------------------------------------------------------------------------------

	private static class TestFormatStub<W> extends AbstractFormatStub<String, W> {

		TestFormatStub(TaskConfig config, ClassLoader classLoader) throws Exception {
			super(config, classLoader);
		}

		@Override
		public <F> Iterator<Pair<String, F>> getFormat(FormatType<F> type) {
			return null;
		}

		@Override
		public Configuration getParameters(String s) {
			return null;
		}
	}

	private static final class TestInputFormat extends GenericInputFormat<Object> {

		private final String name;

		TestInputFormat(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		@Override
		public boolean reachedEnd()  {
			return false;
		}

		@Override
		public Object nextRecord(Object reuse) {
			return null;
		}

		@Override
		public GenericInputSplit[] createInputSplits(int numSplits) {
			return null;
		}
	}

	private static final class TestOutputFormat extends DiscardingOutputFormat<Object> implements InitializeOnMaster, FinalizeOnMaster {

		private final String name;

		TestOutputFormat(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		@Override
		public void initializeGlobal(int parallelism) {

		}

		@Override
		public void finalizeGlobal(int parallelism) {

		}

		@Override
		public void configure(Configuration parameters) {

		}

	}
}
