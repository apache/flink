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

package org.apache.flink.streaming.api.operators.async.queue;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.util.CollectorOutput;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Utility for putting elements inside a {@link StreamElementQueue}.
 */
class QueueUtil {
	static ResultFuture<Integer> putSuccessfully(StreamElementQueue<Integer> queue, StreamElement streamElement) {
		Optional<ResultFuture<Integer>> resultFuture = queue.tryPut(streamElement);
		assertTrue(resultFuture.isPresent());
		return resultFuture.get();
	}

	static void putUnsuccessfully(StreamElementQueue<Integer> queue, StreamElement streamElement) {
		Optional<ResultFuture<Integer>> resultFuture = queue.tryPut(streamElement);
		assertFalse(resultFuture.isPresent());
	}

	/**
	 * Pops all completed elements from the head of this queue.
	 *
	 * @return Completed elements or empty list if none exists.
	 */
	static List<StreamElement> popCompleted(StreamElementQueue<Integer> queue) {
		final List<StreamElement> completed = new ArrayList<>();
		TimestampedCollector<Integer> collector = new TimestampedCollector<>(new CollectorOutput<>(completed));
		while (queue.hasCompletedElements()) {
			queue.emitCompletedElement(collector);
		}
		collector.close();
		return completed;
	}

}
