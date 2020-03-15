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

package org.apache.flink.util;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link TimeUtils#formatWithHighestUnit(Duration)}.
 */
@RunWith(Parameterized.class)
public class TimeUtilsPrettyPrintingTest extends TestLogger {
	@Parameterized.Parameters
	public static Object[][] parameters() {
		return new Object[][]{
			new Object[]{
				Duration.ofMinutes(3).plusSeconds(30),
				"210 s"
			},
			new Object[]{
				Duration.ofNanos(100),
				"100 ns"
			},
			new Object[]{
				Duration.ofSeconds(120),
				"2 min"
			},
			new Object[]{
				Duration.ofMillis(200),
				"200 ms"
			},
			new Object[]{
				Duration.ofHours(1).plusSeconds(3),
				"3603 s"
			},
			new Object[]{
				Duration.ofSeconds(0),
				"0 ms"
			},
			new Object[]{
				Duration.ofMillis(60000),
				"1 min"
			}
		};
	}

	@Parameterized.Parameter
	public Duration duration;

	@Parameterized.Parameter(1)
	public String expectedString;

	@Test
	public void testFormatting() {
		assertThat(TimeUtils.formatWithHighestUnit(duration), is(expectedString));
	}
}
