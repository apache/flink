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

package org.apache.flink.streaming.api.operators.collect.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationResponse;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.function.BooleanSupplier;

/**
 * A simple client for fetching collect results.
 */
public class TestCollectClient<T> extends Thread {

	private static final String INIT_VERSION = "";
	private static final int MAX_RETRY_COUNT = 100;

	private final TypeSerializer<T> serializer;
	private final CollectRequestSender<T> sender;
	private final BooleanSupplier jobFinishedChecker;

	private final LinkedList<T> uncheckpointedResults;
	private final LinkedList<T> checkpointedResults;

	private String version;
	private long offset;
	private long lastCheckpointedOffset;
	private int retryCount;

	public TestCollectClient(
			TypeSerializer<T> serializer,
			CollectRequestSender<T> sender,
			BooleanSupplier jobFinishedChecker) {
		this.serializer = serializer;
		this.sender = sender;
		this.jobFinishedChecker = jobFinishedChecker;

		this.uncheckpointedResults = new LinkedList<>();
		this.checkpointedResults = new LinkedList<>();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void run() {
		Random random = new Random();

		version = INIT_VERSION;
		offset = 0;
		lastCheckpointedOffset = 0;
		retryCount = 0;

		try {
			while (!jobFinishedChecker.getAsBoolean()) {
				if (random.nextBoolean()) {
					Thread.sleep(random.nextInt(10));
				}
				CollectCoordinationResponse<T> response = sender.sendRequest(version, offset);
				dealWithResponse(response, offset);
			}

			Tuple2<Long, CollectCoordinationResponse> accResults = sender.getAccumulatorResults();
			dealWithResponse(accResults.f1, accResults.f0);
			checkpointedResults.addAll(uncheckpointedResults);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public List<T> getResults() {
		return checkpointedResults;
	}

	private void dealWithResponse(CollectCoordinationResponse<T> response, long responseOffset) throws IOException {
		String responseVersion = response.getVersion();
		long responseLastCheckpointedOffset = response.getLastCheckpointedOffset();
		List<T> responseResults = response.getResults(serializer);

		if (responseResults.isEmpty()) {
			retryCount++;
		} else {
			retryCount = 0;
		}
		if (retryCount > MAX_RETRY_COUNT) {
			// not to block the tests
			// throw new RuntimeException("Too many retries in TestCollectClient");
		}

		if (INIT_VERSION.equals(version)) {
			// first response, update version accordingly
			version = responseVersion;
		} else {
			if (responseLastCheckpointedOffset > lastCheckpointedOffset) {
				// a new checkpoint happens
				int newCheckpointedNum = (int) (responseLastCheckpointedOffset - lastCheckpointedOffset);
				for (int i = 0; i < newCheckpointedNum; i++) {
					T result = uncheckpointedResults.removeFirst();
					checkpointedResults.add(result);
				}
				lastCheckpointedOffset = responseLastCheckpointedOffset;
			}

			if (!version.equals(responseVersion)) {
				// sink has restarted
				int removeNum = (int) (offset - lastCheckpointedOffset);
				for (int i = 0; i < removeNum; i++) {
					uncheckpointedResults.removeLast();
				}
				version = responseVersion;
				offset = lastCheckpointedOffset;
			}

			if (responseResults.size() > 0) {
				int addStart = (int) (offset - responseOffset);
				List<T> resultsToAdd = responseResults.subList(addStart, responseResults.size());
				uncheckpointedResults.addAll(resultsToAdd);
				offset += resultsToAdd.size();
			}
		}
	}
}
