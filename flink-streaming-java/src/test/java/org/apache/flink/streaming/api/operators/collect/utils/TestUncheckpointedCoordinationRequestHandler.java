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

package org.apache.flink.streaming.api.operators.collect.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * A {@link CoordinationRequestHandler} to test fetching SELECT query results.
 * It does not do checkpoint and will produce all results again when failure occurs.
 */
public class TestUncheckpointedCoordinationRequestHandler<T> extends AbstractTestCoordinationRequestHandler<T> {

	private int failCount;

	private final LinkedList<T> originalData;
	private LinkedList<T> data;

	public TestUncheckpointedCoordinationRequestHandler(
			int failCount,
			List<T> data,
			TypeSerializer<T> serializer,
			String accumulatorName) {
		super(serializer, accumulatorName);
		this.failCount = failCount;

		this.originalData = new LinkedList<>(data);
		this.data = new LinkedList<>(data);

		this.closed = false;
	}

	@Override
	protected void updateBufferedResults() {
		for (int i = random.nextInt(3) + 1; i > 0; i--) {
			int r = random.nextInt(20);
			if (r < 19 || failCount <= 0) {
				// with 95% chance we add data
				int size = Math.min(data.size(), BATCH_SIZE * 2 - buffered.size());
				if (size > 0) {
					size = random.nextInt(size) + 1;
				}
				for (int j = 0; j < size; j++) {
					buffered.add(data.removeFirst());
				}

				if (data.isEmpty()) {
					buildAccumulatorResults();
					closed = true;
					break;
				}
			} else {
				// with 5% chance we fail, we fail at most `failCount` times
				failCount--;

				// we shuffle data to simulate jobs whose result order is undetermined
				data = new LinkedList<>(originalData);
				Collections.shuffle(data);

				buffered = new LinkedList<>();
				version = UUID.randomUUID().toString();
				offset = 0;
			}
		}
	}
}
