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


package org.apache.flink.optimizer.plandump;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class NumberFormattingTest {

	@Test
	public void testFormatNumberNoDigit() {
		assertEquals("0.0", PlanJSONDumpGenerator.formatNumber(0));
		assertEquals("0.00", PlanJSONDumpGenerator.formatNumber(0.0000000001));
		assertEquals("-1.0", PlanJSONDumpGenerator.formatNumber(-1.0));
		assertEquals("1.00", PlanJSONDumpGenerator.formatNumber(1));
		assertEquals("17.00", PlanJSONDumpGenerator.formatNumber(17));
		assertEquals("17.44", PlanJSONDumpGenerator.formatNumber(17.44));
		assertEquals("143.00", PlanJSONDumpGenerator.formatNumber(143));
		assertEquals("143.40", PlanJSONDumpGenerator.formatNumber(143.4));
		assertEquals("143.50", PlanJSONDumpGenerator.formatNumber(143.5));
		assertEquals("143.60", PlanJSONDumpGenerator.formatNumber(143.6));
		assertEquals("143.45", PlanJSONDumpGenerator.formatNumber(143.45));
		assertEquals("143.55", PlanJSONDumpGenerator.formatNumber(143.55));
		assertEquals("143.65", PlanJSONDumpGenerator.formatNumber(143.65));
		assertEquals("143.66", PlanJSONDumpGenerator.formatNumber(143.655));
		
		assertEquals("1.13 K", PlanJSONDumpGenerator.formatNumber(1126.0));
		assertEquals("11.13 K", PlanJSONDumpGenerator.formatNumber(11126.0));
		assertEquals("118.13 K", PlanJSONDumpGenerator.formatNumber(118126.0));

		assertEquals("1.44 M", PlanJSONDumpGenerator.formatNumber(1435126.0));
	}
	
	
}
