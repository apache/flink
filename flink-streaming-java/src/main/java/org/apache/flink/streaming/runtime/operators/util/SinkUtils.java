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

package org.apache.flink.streaming.runtime.operators.util;

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Utility class for {@link org.apache.flink.api.connector.sink.Sink}'s runtime operators.
 */
public class SinkUtils {

	protected static final Logger LOG = LoggerFactory.getLogger(SinkUtils.class);

	/**
	 * Commit a list of committables until all of them are committed successfully if retryInterval > 0.
	 * or return a list of uncommitted committables.
	 *
	 * @param committables A list of committables
	 * @param committer A function used to commit the committables.
	 * @param output Used to send the committed committables.
	 * @param retryInterval Commit retry interval.
	 *
	 * @return A list of uncommitted committables.
	 */
	public static <CommT> List<CommT> commit(
			List<CommT> committables,
			Function<List<CommT>, List<CommT>> committer,
			@Nullable Output<StreamRecord<CommT>> output,
			long retryInterval) throws InterruptedException {

		final List<CommT> readyCommittables = new ArrayList<>(committables);
		final List<CommT> neededToRetryCommittables = new ArrayList<>();
		boolean retry;
		do {
			retry = false;
			readyCommittables.addAll(neededToRetryCommittables);
			neededToRetryCommittables.clear();
			neededToRetryCommittables.addAll(committer.apply(Collections.unmodifiableList(
					readyCommittables)));
			if (!neededToRetryCommittables.isEmpty()) {
				LOG.info(
						"{} of {} committables needed to retry",
						neededToRetryCommittables.size(),
						readyCommittables.size());
				readyCommittables.removeAll(neededToRetryCommittables);
			}
			if (output != null) {
				for (CommT committable : readyCommittables) {
					output.collect(new StreamRecord<>(committable));
				}
			}
			readyCommittables.clear();
			if (!neededToRetryCommittables.isEmpty() && retryInterval > 0) {
				retry = true;
				Thread.sleep(retryInterval);
			}
		} while (retry);

		return neededToRetryCommittables;
	}
}
