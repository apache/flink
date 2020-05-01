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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.connectors.kafka.internal.AlignmentTimestampAggregateFunction;

import javafx.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Tests for the {@link AlignmentTimestampAggregateFunction}.
 */
public class AlignmentTimestampAggregateFunctionTest {

	@Test
	public void alignmentTsAggregatorTest() {
		AlignmentTimestampAggregateFunction aggregator = new AlignmentTimestampAggregateFunction();
		Map<Integer, Long> accumulator = aggregator.createAccumulator();

		accumulator = aggregator.add(new Pair<>(1, 10L), accumulator);
		accumulator = aggregator.add(new Pair<>(2, 20L), accumulator);
		accumulator = aggregator.add(new Pair<>(3, 30L), accumulator);

		Assert.assertEquals("Aggregator result should be min of all tasks",
			10L, (long) aggregator.getResult(accumulator));

		accumulator = aggregator.add(new Pair<>(1, 25L), accumulator);
		Assert.assertEquals("Aggregator result should be updated to min of all tasks",
			20L, (long) aggregator.getResult(accumulator));
	}

	@Test(expected = NoSuchElementException.class)
	public void emptyAccumulatorTest() {
		// We haven't handled empty accumulator because globalAggregateManager.updateGlobalAggregate
		// always does a aggregator.add before aggregator.getResult
		AlignmentTimestampAggregateFunction aggregator = new AlignmentTimestampAggregateFunction();
		aggregator.getResult(aggregator.createAccumulator());
	}

}
