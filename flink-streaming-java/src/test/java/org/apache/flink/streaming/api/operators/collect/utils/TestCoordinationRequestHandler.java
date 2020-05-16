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

import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationRequest;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationResponse;
import org.apache.flink.streaming.api.operators.collect.CollectSinkFunction;
import org.apache.flink.util.OptionalFailure;

import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link CoordinationRequestHandler} to test fetching SELECT query results.
 */
public class TestCoordinationRequestHandler<T> implements CoordinationRequestHandler {

	private static final int BATCH_SIZE = 3;

	private final TypeSerializer<T> serializer;
	private final String accumulatorName;

	private int checkpointCountDown;

	private LinkedList<T> data;
	private List<T> checkpointingData;
	private List<T> checkpointedData;

	private LinkedList<T> buffered;
	private List<T> checkpointingBuffered;
	private List<T> checkpointedBuffered;

	private String version;

	private long offset;
	private long checkpointingOffset;
	private long checkpointedOffset;

	private Map<String, OptionalFailure<Object>> accumulatorResults;

	private Random random;
	private boolean closed;

	public TestCoordinationRequestHandler(
			List<T> data,
			TypeSerializer<T> serializer,
			String accumulatorName) {
		this.serializer = serializer;
		this.accumulatorName = accumulatorName;

		this.checkpointCountDown = 0;

		this.data = new LinkedList<>(data);
		this.checkpointedData = new ArrayList<>(data);

		this.buffered = new LinkedList<>();
		this.checkpointedBuffered = new ArrayList<>();

		this.version = UUID.randomUUID().toString();

		this.offset = 0;
		this.checkpointingOffset = 0;
		this.checkpointedOffset = 0;

		this.accumulatorResults = new HashMap<>();

		this.random = new Random();
		this.closed = false;
	}

	@Override
	public CompletableFuture<CoordinationResponse> handleCoordinationRequest(CoordinationRequest request) {
		if (closed) {
			throw new RuntimeException("Handler closed");
		}

		Assert.assertTrue(request instanceof CollectCoordinationRequest);
		CollectCoordinationRequest collectRequest = (CollectCoordinationRequest) request;

		for (int i = random.nextInt(3) + 1; i > 0; i--) {
			if (checkpointCountDown > 0) {
				checkpointCountDown--;
				if (checkpointCountDown == 0) {
					checkpointedData = checkpointingData;
					checkpointedBuffered = checkpointingBuffered;
					checkpointedOffset = checkpointingOffset;
				}
			}

			int r = random.nextInt(10);
			if (r < 6) {
				// with 60% chance we add data
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
			} else if (r < 9) {
				// with 30% chance we do a checkpoint completed in the future
				if (checkpointCountDown == 0) {
					checkpointCountDown = random.nextInt(5) + 1;
					checkpointingData = new ArrayList<>(data);
					checkpointingBuffered = new ArrayList<>(buffered);
					checkpointingOffset = offset;
				}
			} else {
				// with 10% chance we fail
				checkpointCountDown = 0;
				version = UUID.randomUUID().toString();
				data = new LinkedList<>(checkpointedData);
				buffered = new LinkedList<>(checkpointedBuffered);
				offset = checkpointedOffset;
			}
		}

		Assert.assertTrue(offset <= collectRequest.getOffset());

		List<T> subList = Collections.emptyList();
		if (collectRequest.getVersion().equals(version)) {
			while (buffered.size() > 0 && collectRequest.getOffset() > offset) {
				buffered.removeFirst();
				offset++;
			}
			subList = new ArrayList<>();
			Iterator<T> iterator = buffered.iterator();
			for (int i = 0; i < BATCH_SIZE && iterator.hasNext(); i++) {
				subList.add(iterator.next());
			}
		}

		CoordinationResponse response;
		try {
			if (random.nextBoolean()) {
				// with 50% chance we return valid result
				response = new CollectCoordinationResponse<>(version, checkpointedOffset, subList, serializer);
			} else {
				// with 50% chance we return invalid result
				response = new CollectCoordinationResponse<>(
					collectRequest.getVersion(),
					-1,
					Collections.emptyList(),
					serializer);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return CompletableFuture.completedFuture(response);
	}

	public boolean isClosed() {
		return closed;
	}

	public Map<String, OptionalFailure<Object>> getAccumulatorResults() {
		return accumulatorResults;
	}

	private void buildAccumulatorResults() {
		List<T> finalResults = new ArrayList<>(buffered);
		SerializedListAccumulator<byte[]> listAccumulator = new SerializedListAccumulator<>();
		try {
			byte[] serializedResult =
				CollectSinkFunction.serializeAccumulatorResult(
					offset, version, checkpointedOffset, finalResults, serializer);
			listAccumulator.add(serializedResult, BytePrimitiveArraySerializer.INSTANCE);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		accumulatorResults.put(accumulatorName, OptionalFailure.of(listAccumulator.getLocalValue()));
	}
}
