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

package org.apache.flink.metrics;

import org.junit.Test;

/**
 * Unit test for {@link MetricDef}.
 */
public class MetricDefTest {

	@Test(expected = IllegalStateException.class)
	public void testDuplicateDef() {
		new MetricDef()
			.define("name", "doc", MetricSpec.counter())
			.define("name", "doc", MetricSpec.counter());
	}

	@Test(expected = IllegalStateException.class)
	public void testDuplicateSubMetricDef() {
		new MetricDef()
			.define("name1", "doc", MetricSpec.meter("submetric"))
			.define("name2", "doc", MetricSpec.meter("submetric"));
	}

	@Test(expected = IllegalStateException.class)
	public void testCyclicDependency() {
		new MetricDef()
			.define("name1", "doc1", MetricSpec.meter("name2"))
			.define("name2", "doc2", MetricSpec.meter("name3"))
			.define("name3", "doc3", MetricSpec.meter("name1"));
	}
}
