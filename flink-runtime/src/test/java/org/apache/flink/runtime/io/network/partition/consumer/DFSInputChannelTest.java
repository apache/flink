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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class DFSInputChannelTest {

	@Test
	public void testOnBufferAndGet() throws Exception {
		// Setup
		final SingleInputGate inputGate = mock(SingleInputGate.class);
		final DFSInputChannel inputChannel = createDFSInputChannel(inputGate);

		// The test
		inputChannel.onBuffer(TestBufferFactory.getMockBuffer());

		assertTrue(inputChannel.getNextBuffer() != null);
	}

	// ---------------------------------------------------------------------------------------------

	private DFSInputChannel createDFSInputChannel(SingleInputGate inputGate)
			throws IOException, InterruptedException {
		return new DFSInputChannel(
			inputGate,
			0,
			new ResultPartitionID(),
			new JobID(),
			new Tuple2<Integer, Integer>(0, 0),
			new UnregisteredTaskMetricsGroup.DummyIOMetricGroup());
	}
}
