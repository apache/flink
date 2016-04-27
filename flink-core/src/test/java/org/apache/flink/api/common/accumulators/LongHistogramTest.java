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

package org.apache.flink.api.common.accumulators;

import org.junit.Test;
import java.util.TreeMap;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

public class LongHistogramTest {

	private static final Integer KEY = 1;
	public static final long MORE_THAN_AN_INT = (long) Integer.MAX_VALUE + 1;

	@Test
	public void canDealWithLongValue() {
		LongHistogram histogram = new LongHistogram();
		for (long i = 0; i < MORE_THAN_AN_INT; i++) {
			histogram.add(KEY);
		}

		TreeMap<Integer, Long> expectedMap = new TreeMap<>();
		expectedMap.put(KEY, MORE_THAN_AN_INT);

		assertThat(histogram.getLocalValue(), equalTo(expectedMap));
	}

}
