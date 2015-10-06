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

package org.apache.flink.stormcompatibility.api.metrics;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class FlinkCountMetricTest {

	@Test
	public void testAll() {
		LongCounter longCounter = new LongCounter();
		FlinkCountMetric flinkCount = new FlinkCountMetric();
		flinkCount.setCounter(longCounter);
		flinkCount.incr();
		assertEquals(1L, flinkCount.getValueAndReset());
		flinkCount.incrBy(1);
		assertEquals(1L, flinkCount.getValueAndReset());
		assertEquals(0L, flinkCount.getValueAndReset());
	}
}
