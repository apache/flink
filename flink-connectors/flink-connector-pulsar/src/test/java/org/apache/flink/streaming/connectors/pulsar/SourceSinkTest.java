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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.streaming.connectors.pulsar.internal.SerializableRange;
import org.apache.flink.streaming.connectors.pulsar.internal.SourceSinkUtils;
import org.apache.flink.util.TestLogger;

import org.apache.pulsar.client.api.Range;
import org.junit.Assert;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Source unit tests.
 */
public class SourceSinkTest extends TestLogger {

	@Test
	public void testDistributeRange() throws InterruptedException {
		List<CompletableFuture<Void>> futureList = new ArrayList<>(99);
		for (int countOfSubTasks = 1; countOfSubTasks < 100; countOfSubTasks++) {
			final int count = countOfSubTasks;
			futureList.add(CompletableFuture.runAsync(() -> testRange(count)));
		}
		CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
	}

	private void testRange(int countOfSubTasks) {
		//log.info("test pulsar range {}", countOfSubTasks);
		List<Range> ranges = new ArrayList<>(countOfSubTasks);
		for (int i = 0; i < countOfSubTasks; i++) {
			ranges.add(SourceSinkUtils.distributeRange(countOfSubTasks, i));
		}
		//log.info(ranges.toString());
		Collections.sort(ranges, Comparator.comparingInt(Range::getStart));
		Assert.assertEquals(ranges.get(0).getStart(), 0);
		Assert.assertEquals(ranges.get(ranges.size() - 1).getEnd(), SerializableRange.fullRangeEnd);
		for (int i = 1; i < ranges.size(); i++) {
			final Range range = ranges.get(i - 1);
			final Range currentRange = ranges.get(i);
			final String message = MessageFormat.format(
				"countOfSubTasks {0} / indexOfSubTasks {1} \n all range {2}",
				countOfSubTasks,
				i,
				ranges);
			Assert.assertEquals(
				message,
				range.getEnd() + 1,
				currentRange.getStart());
		}
	}
}
