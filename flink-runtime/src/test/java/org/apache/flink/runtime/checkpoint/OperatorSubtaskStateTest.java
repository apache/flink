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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertFalse;

/**
 * {@link OperatorSubtaskState} test.
 */
public class OperatorSubtaskStateTest {
	@Test
	public void testDiscardDuplicatedDelegatesOnce() {
		StreamStateHandle delegate = new DiscardOnceStreamStateHandle();
		new OperatorSubtaskState(
			StateObjectCollection.empty(),
			StateObjectCollection.empty(),
			StateObjectCollection.empty(),
			StateObjectCollection.empty(),
			new StateObjectCollection<>(asList(buildInputChannelHandle(delegate, 1), buildInputChannelHandle(delegate, 2))),
			new StateObjectCollection<>(asList(buildSubpartitionHandle(delegate, 4), buildSubpartitionHandle(delegate, 3)))
		).discardState();
	}

	private ResultSubpartitionStateHandle buildSubpartitionHandle(StreamStateHandle delegate, int subPartitionIdx1) {
		return new ResultSubpartitionStateHandle(new ResultSubpartitionInfo(0, subPartitionIdx1), delegate, singletonList(0L));
	}

	private InputChannelStateHandle buildInputChannelHandle(StreamStateHandle delegate, int inputChannelIdx) {
		return new InputChannelStateHandle(new InputChannelInfo(0, inputChannelIdx), delegate, singletonList(0L));
	}

	private static class DiscardOnceStreamStateHandle extends ByteStreamStateHandle {
		private static final long serialVersionUID = 1L;

		private boolean discarded = false;

		DiscardOnceStreamStateHandle() {
			super("test", new byte[0]);
		}

		@Override
		public void discardState() {
			super.discardState();
			assertFalse("state was discarded twice", discarded);
			discarded = true;
		}
	}
}
