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

import java.io.IOException;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.java.io.DiscardingOuputFormat;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.junit.Test;

import static org.junit.Assert.*;


@SuppressWarnings("serial")
public class JobTaskVertexTest {

	@Test
	public void testConnectDirectly() {
		AbstractJobVertex source = new AbstractJobVertex("source");
		AbstractJobVertex target = new AbstractJobVertex("target");
		target.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE);
		
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
		AbstractJobVertex source = new AbstractJobVertex("source");
		AbstractJobVertex target1= new AbstractJobVertex("target1");
		AbstractJobVertex target2 = new AbstractJobVertex("target2");
		target1.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE);
		target2.connectDataSetAsInput(source.getProducedDataSets().get(0), DistributionPattern.BIPARTITE);
		
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
			final TestingOutputFormat outputFormat = new TestingOutputFormat();
			final OutputFormatVertex of = new OutputFormatVertex("Name");
			new TaskConfig(of.getConfiguration()).setStubWrapper(new UserCodeObjectWrapper<OutputFormat<?>>(outputFormat));
			final ClassLoader cl = getClass().getClassLoader();
			
			try {
				of.initializeOnMaster(cl);
				fail("Did not throw expected exception.");
			} catch (TestException e) {
				// all good
			}
			
			OutputFormatVertex copy = SerializationUtils.clone(of);
			try {
				copy.initializeOnMaster(cl);
				fail("Did not throw expected exception.");
			} catch (TestException e) {
				// all good
			}
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
			InputSplit[] splits = vertex.getInputSplitSource().createInputSplits(77);
			
			assertNotNull(splits);
			assertEquals(1, splits.length);
			assertEquals(TestSplit.class, splits[0].getClass());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final class TestingOutputFormat extends DiscardingOuputFormat<Object> implements InitializeOnMaster {
		@Override
		public void initializeGlobal(int parallelism) throws IOException {
			throw new TestException();
		}
	}
	
	private static final class TestException extends IOException {}
	
	// --------------------------------------------------------------------------------------------
	
	private static final class TestSplit extends GenericInputSplit {}
	
	private static final class TestInputFormat extends GenericInputFormat<Object> {

		@Override
		public boolean reachedEnd()  {
			return false;
		}

		@Override
		public Object nextRecord(Object reuse) {
			return null;
		}
		
		@Override
		public GenericInputSplit[] createInputSplits(int numSplits) throws IOException {
			return new GenericInputSplit[] { new TestSplit() };
		}
	}
}
