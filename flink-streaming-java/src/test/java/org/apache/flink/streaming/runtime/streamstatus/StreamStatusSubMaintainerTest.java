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

package org.apache.flink.streaming.runtime.streamstatus;

import org.junit.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static org.apache.flink.streaming.runtime.streamstatus.StreamStatus.ACTIVE;
import static org.apache.flink.streaming.runtime.streamstatus.StreamStatus.IDLE;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link StreamStatusSubMaintainer}.
 */
public class StreamStatusSubMaintainerTest {

	@Test
	public void testStreamStatusMaintaining() {
		final StreamStatusMaintainer parentMaintainer = new StreamStatusMaintainerInstance();
		final BitSet subStatus = new BitSet();
		final List<StreamStatusSubMaintainer> subMaintainers = new ArrayList<>();

		for (int i = 0; i < 10; i++) {
			final StreamStatusSubMaintainer subMaintainer = new StreamStatusSubMaintainer(parentMaintainer, subStatus, i);
			assertEquals(ACTIVE, subMaintainer.getStreamStatus());
			subMaintainers.add(subMaintainer);
		}

		for (StreamStatusSubMaintainer subMaintainer : subMaintainers) {
			assertEquals(ACTIVE, parentMaintainer.getStreamStatus());
			subMaintainer.toggleStreamStatus(IDLE);
			assertEquals(IDLE, subMaintainer.getStreamStatus());
		}
		assertEquals(IDLE, parentMaintainer.getStreamStatus());
	}

	@Test
	public void testStreamStatusFinishedMaintaining() {
		final StreamStatusMaintainer parentMaintainer = new StreamStatusMaintainerInstance();
		final BitSet subStatus = new BitSet();
		final List<StreamStatusSubMaintainer> subMaintainers = new ArrayList<>();

		for (int i = 0; i < 10; i++) {
			final StreamStatusSubMaintainer subMaintainer = new StreamStatusSubMaintainer(parentMaintainer, subStatus, i);
			assertEquals(ACTIVE, subMaintainer.getStreamStatus());
			assertEquals(ACTIVE, parentMaintainer.getStreamStatus());
			subMaintainers.add(subMaintainer);
		}

		for (int i = 0; i < 5; i++) {
			subMaintainers.get(i).release();
		}

		for (int i = 5; i < 10; i++) {
			final StreamStatusSubMaintainer subMaintainer = subMaintainers.get(i);
			assertEquals(ACTIVE, parentMaintainer.getStreamStatus());
			subMaintainer.toggleStreamStatus(IDLE);
			assertEquals(IDLE, subMaintainer.getStreamStatus());
		}
		// The parent would be IDLE if all sub maintainers are IDLE
		assertEquals(IDLE, parentMaintainer.getStreamStatus());

		subMaintainers.get(0).toggleStreamStatus(ACTIVE);

		// The parent would be ACTIVE if there is a sub maintainer active
		assertEquals(ACTIVE, parentMaintainer.getStreamStatus());
	}

	class StreamStatusMaintainerInstance implements StreamStatusMaintainer {

		public StreamStatus streamStatus = ACTIVE;

		@Override
		public StreamStatus getStreamStatus() {
			return streamStatus;
		}

		@Override
		public void toggleStreamStatus(StreamStatus streamStatus) {
			this.streamStatus = streamStatus;
		}
	}
}
