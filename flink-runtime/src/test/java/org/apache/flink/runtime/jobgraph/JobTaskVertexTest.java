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
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link JobVertex} and all its inheritance classes.
 */
@SuppressWarnings("serial")
public class JobTaskVertexTest {

	@Test
	public void testConnectDirectly() {
		JobVertex source = new JobVertex("source");
		JobVertex target = new JobVertex("target");
		target.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		assertTrue(source.isInputVertex());
		assertFalse(source.isOutputVertex());
		assertFalse(target.isInputVertex());
		assertTrue(target.isOutputVertex());

		assertEquals(1, source.getNumberOfProducedIntermediateDataSets());
		assertEquals(1, target.getNumberOfInputs());

		assertEquals(target.getInputs().get(0).getSource(), source.getProducedDataSets().get(0));

		assertEquals(1, source.getProducedDataSets().get(0).getConsumers().size());
		assertEquals(target, source.getProducedDataSets().get(0).getConsumers().get(0).getTarget());
	}

	@Test
	public void testConnectMultipleTargets() {
		JobVertex source = new JobVertex("source");
		JobVertex target1 = new JobVertex("target1");
		JobVertex target2 = new JobVertex("target2");
		target1.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		target2.connectDataSetAsInput(source.getProducedDataSets().get(0), DistributionPattern.ALL_TO_ALL);

		assertTrue(source.isInputVertex());
		assertFalse(source.isOutputVertex());
		assertFalse(target1.isInputVertex());
		assertTrue(target1.isOutputVertex());
		assertFalse(target2.isInputVertex());
		assertTrue(target2.isOutputVertex());

		assertEquals(1, source.getNumberOfProducedIntermediateDataSets());
		assertEquals(2, source.getProducedDataSets().get(0).getConsumers().size());

		assertEquals(target1.getInputs().get(0).getSource(), source.getProducedDataSets().get(0));
		assertEquals(target2.getInputs().get(0).getSource(), source.getProducedDataSets().get(0));
	}

	@Test
	public void testOutputFormatVertex() {
		try {
			final OutputFormat outputFormat = new TestingOutputFormat();
			final OutputFormatVertex of = new OutputFormatVertex("Name");
			new TaskConfig(of.getConfiguration()).setStubWrapper(new UserCodeObjectWrapper<OutputFormat<?>>(outputFormat));
			final ClassLoader cl = new TestClassLoader();

			try {
				of.initializeOnMaster(cl);
				fail("Did not throw expected exception.");
			} catch (Throwable t) {
				assertTrue(ExceptionUtils.findThrowable(t, TestException.class).isPresent());
			}

			OutputFormatVertex copy = InstantiationUtil.clone(of);
			ClassLoader ctxCl = Thread.currentThread().getContextClassLoader();
			try {
				copy.initializeOnMaster(cl);
				fail("Did not throw expected exception.");
			} catch (Throwable t) {
				assertTrue(ExceptionUtils.findThrowable(t, TestException.class).isPresent());
			}
			assertEquals("Previous classloader was not restored.", ctxCl, Thread.currentThread().getContextClassLoader());

			try {
				copy.finalizeOnMaster(cl);
				fail("Did not throw expected exception.");
			} catch (Throwable t) {
				assertTrue(ExceptionUtils.findThrowable(t, TestException.class).isPresent());
			}
			assertEquals("Previous classloader was not restored.", ctxCl, Thread.currentThread().getContextClassLoader());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testInputFormatVertex() {
		try {
			final TestInputFormat inputFormat = new TestInputFormat();
			final InputFormatVertex vertex = new InputFormatVertex("Name");
			new TaskConfig(vertex.getConfiguration()).setStubWrapper(new UserCodeObjectWrapper<InputFormat<?, ?>>(inputFormat));

			final ClassLoader cl = getClass().getClassLoader();

			vertex.initializeOnMaster(cl);

			OperatorID sourceOperatorID = OperatorID.fromJobVertexID(vertex.getID());
			Map<OperatorID, InputSplitSource<?>> inputSplitSourceMap = vertex.getInputSplitSources();
			assertEquals(1, inputSplitSourceMap.size());
			assertTrue(inputSplitSourceMap.containsKey(sourceOperatorID));

			InputSplit[] splits = inputSplitSourceMap.get(sourceOperatorID).createInputSplits(77);
			assertNotNull(splits);
			assertEquals(1, splits.length);
			assertEquals(TestSplit.class, splits[0].getClass());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testMultiInputOutputFormatVertex() {
		try {
			OperatorID inputOperatorID1 = new OperatorID();
			OperatorID inputOperatorID2 = new OperatorID();
			OperatorID outputOperatorID1 = new OperatorID();
			OperatorID outputOperatorID2 = new OperatorID();

			final Map<OperatorID, InputFormat> inputFormatMap = new HashMap<>();
			inputFormatMap.put(inputOperatorID1, new TestInputFormat("test input format 1"));
			inputFormatMap.put(inputOperatorID2, new TestInputFormat("test input format 2"));

			final Map<OperatorID, OutputFormat> outputFormatMap = new HashMap<>();
			outputFormatMap.put(outputOperatorID1, new TestOutputFormatWithCalledCount());
			outputFormatMap.put(outputOperatorID2, new TestOutputFormatWithCalledCount());

			final MultiInputOutputFormatVertex vertex = new MultiInputOutputFormatVertex("Name");
			FormatUtil.MultiFormatStub.setStubFormats(new TaskConfig(vertex.getConfiguration()), inputFormatMap, outputFormatMap);

			ClassLoader cl = getClass().getClassLoader();

			TestOutputFormatWithCalledCount.resetCalledCount();

			// test initializeOnMaster()
			vertex.initializeOnMaster(cl);

			Map<OperatorID, InputSplitSource<?>> inputSplitSourceMap = vertex.getInputSplitSources();
			assertEquals(2, inputSplitSourceMap.size());
			assertTrue(inputSplitSourceMap.containsKey(inputOperatorID1));
			assertTrue(inputSplitSourceMap.containsKey(inputOperatorID2));

			TestInputFormat inputFormat1 = (TestInputFormat) inputSplitSourceMap.get(inputOperatorID1);
			assertEquals("test input format 1", inputFormat1.getName());
			InputSplit[] splits1 = inputFormat1.createInputSplits(77);
			assertNotNull(splits1);
			assertEquals(1, splits1.length);
			assertEquals(TestSplit.class, splits1[0].getClass());

			TestInputFormat inputFormat2 = (TestInputFormat) inputSplitSourceMap.get(inputOperatorID2);
			assertEquals("test input format 2", inputFormat2.getName());
			InputSplit[] splits2 = inputFormat2.createInputSplits(77);
			assertNotNull(splits2);
			assertEquals(1, splits2.length);
			assertEquals(TestSplit.class, splits2[0].getClass());

			assertEquals(2, TestOutputFormatWithCalledCount.getTotalInitializeCalledCount());
			assertEquals(0, TestOutputFormatWithCalledCount.getTotalFinalizeCalledCount());

			// test finalizeOnMaster()
			vertex.finalizeOnMaster(cl);

			assertEquals(2, TestOutputFormatWithCalledCount.getTotalInitializeCalledCount());
			assertEquals(2, TestOutputFormatWithCalledCount.getTotalFinalizeCalledCount());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testNonFormatVertex() {
		try {
			final JobVertex vertex = new JobVertex("Name");
			ClassLoader cl = getClass().getClassLoader();

			vertex.initializeOnMaster(cl);
			assertNull(vertex.getInputSplitSources());

			vertex.finalizeOnMaster(cl);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// --------------------------------------------------------------------------------------------

	private static final class TestException extends IOException {}

	private static final class TestSplit extends GenericInputSplit {

		TestSplit(int partitionNumber, int totalNumberOfPartitions) {
			super(partitionNumber, totalNumberOfPartitions);
		}
	}

	private static final class TestInputFormat extends GenericInputFormat<Object> {

		private final String name;

		TestInputFormat() {
			this(null);
		}

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
			return new GenericInputSplit[] { new TestSplit(0, 1) };
		}
	}

	private static final class TestingOutputFormat extends DiscardingOutputFormat<Object> implements InitializeOnMaster, FinalizeOnMaster {

		private boolean isConfigured = false;

		@Override
		public void initializeGlobal(int parallelism) throws IOException {
			if (!isConfigured) {
				throw new IllegalStateException("OutputFormat was not configured before initializeGlobal was called.");
			}
			if (!(Thread.currentThread().getContextClassLoader() instanceof TestClassLoader)) {
				throw new IllegalStateException("Context ClassLoader was not correctly switched.");
			}
			// notify we have been here.
			throw new TestException();
		}

		@Override
		public void finalizeGlobal(int parallelism) throws IOException {
			if (!isConfigured) {
				throw new IllegalStateException("OutputFormat was not configured before finalizeGlobal was called.");
			}
			if (!(Thread.currentThread().getContextClassLoader() instanceof TestClassLoader)) {
				throw new IllegalStateException("Context ClassLoader was not correctly switched.");
			}
			// notify we have been here.
			throw new TestException();
		}

		@Override
		public void configure(Configuration parameters) {
			if (isConfigured) {
				throw new IllegalStateException("OutputFormat is already configured.");
			}
			if (!(Thread.currentThread().getContextClassLoader() instanceof TestClassLoader)) {
				throw new IllegalStateException("Context ClassLoader was not correctly switched.");
			}
			isConfigured = true;
		}

	}

	private static class TestClassLoader extends URLClassLoader {
		TestClassLoader() {
			super(new URL[0], Thread.currentThread().getContextClassLoader());
		}
	}

	private static final class TestOutputFormatWithCalledCount extends DiscardingOutputFormat<Object> implements InitializeOnMaster, FinalizeOnMaster {

		private static transient int totalInitializeCalledCount;
		private static transient int totalFinalizeCalledCount;

		private boolean isConfigured = false;

		@Override
		public void initializeGlobal(int parallelism) throws IllegalStateException {
			if (!isConfigured) {
				throw new IllegalStateException("OutputFormat was not configured before initializeGlobal was called.");
			}

			totalInitializeCalledCount++;
		}

		@Override
		public void finalizeGlobal(int parallelism) throws IllegalStateException {
			if (!isConfigured) {
				throw new IllegalStateException("OutputFormat was not configured before finalizeGlobal was called.");
			}

			totalFinalizeCalledCount++;
		}

		@Override
		public void configure(Configuration parameters) throws IllegalStateException {
			if (isConfigured) {
				throw new IllegalStateException("OutputFormat is already configured.");
			}

			isConfigured = true;
		}

		static int getTotalInitializeCalledCount() {
			return totalInitializeCalledCount;
		}

		static int getTotalFinalizeCalledCount() {
			return totalFinalizeCalledCount;
		}

		static void resetCalledCount() {
			totalInitializeCalledCount = 0;
			totalFinalizeCalledCount = 0;
		}
	}
}
