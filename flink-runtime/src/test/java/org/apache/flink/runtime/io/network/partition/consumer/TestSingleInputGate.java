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
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.taskmanager.TaskActions;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.util.ArrayDeque;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * A test input gate to mock reading data.
 */
public class TestSingleInputGate {

	protected final SingleInputGate inputGate;

	protected final TestInputChannel[] inputChannels;

	public TestSingleInputGate(int numberOfInputChannels) {
		this(numberOfInputChannels, true);
	}

	public TestSingleInputGate(int numberOfInputChannels, boolean initialize) {
		checkArgument(numberOfInputChannels >= 1);

		SingleInputGate realGate = new SingleInputGate(
			"Test Task Name",
			new JobID(),
			new IntermediateDataSetID(),
			ResultPartitionType.PIPELINED,
			0,
			numberOfInputChannels,
			mock(TaskActions.class),
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup(),
			true);

		this.inputGate = spy(realGate);

		// Notify about late registrations (added for DataSinkTaskTest#testUnionDataSinkTask).
		// After merging registerInputOutput and invoke, we have to make sure that the test
		// notifications happen at the expected time. In real programs, this is guaranteed by
		// the instantiation and request partition life cycle.
		try {
			Field f = realGate.getClass().getDeclaredField("inputChannelsWithData");
			f.setAccessible(true);
			final ArrayDeque<InputChannel> notifications = (ArrayDeque<InputChannel>) f.get(realGate);

			doAnswer(new Answer<Void>() {
				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					invocation.callRealMethod();

					synchronized (notifications) {
						if (!notifications.isEmpty()) {
							InputGateListener listener = (InputGateListener) invocation.getArguments()[0];
							listener.notifyInputGateNonEmpty(inputGate);
						}
					}

					return null;
				}
			}).when(inputGate).registerListener(any(InputGateListener.class));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		this.inputChannels = new TestInputChannel[numberOfInputChannels];

		if (initialize) {
			for (int i = 0; i < numberOfInputChannels; i++) {
				inputChannels[i] = new TestInputChannel(inputGate, i);
				inputGate.setInputChannel(new IntermediateResultPartitionID(), inputChannels[i]);
			}
		}
	}

	public SingleInputGate getInputGate() {
		return inputGate;
	}

}
