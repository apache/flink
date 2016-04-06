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

package org.apache.flink.streaming.api;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.StatefulSequenceSource;
import org.apache.flink.streaming.util.SourceFunctionUtil;
import org.junit.Test;

public class SourceFunctionTest {

	@Test
	public void fromElementsTest() throws Exception {
		List<Integer> expectedList = Arrays.asList(1, 2, 3);
		List<Integer> actualList = SourceFunctionUtil.runSourceFunction(CommonTestUtils.createCopySerializable(
				new FromElementsFunction<Integer>(
						IntSerializer.INSTANCE,
						1,
						2,
						3)));
		assertEquals(expectedList, actualList);
	}

	@Test
	public void fromCollectionTest() throws Exception {
		List<Integer> expectedList = Arrays.asList(1, 2, 3);
		List<Integer> actualList = SourceFunctionUtil.runSourceFunction(
				CommonTestUtils.createCopySerializable(new FromElementsFunction<Integer>(
						IntSerializer.INSTANCE,
						Arrays.asList(1, 2, 3))));
		assertEquals(expectedList, actualList);
	}

	@Test
	public void generateSequenceTest() throws Exception {
		List<Long> expectedList = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
		List<Long> actualList = SourceFunctionUtil.runSourceFunction(new StatefulSequenceSource(1,
				7));
		assertEquals(expectedList, actualList);
	}
}
