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
package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class AbstractMetricGroupTest {
	/**
	 * Verifies that no {@link NullPointerException} is thrown when {@link AbstractMetricGroup#getAllVariables()} is
	 * called and the parent is null.
	 */
	@Test
	public void testGetAllVariables() {
		MetricRegistry registry = new MetricRegistry(new Configuration());

		AbstractMetricGroup group = new AbstractMetricGroup<AbstractMetricGroup<?>>(registry, new String[0], null) {
			@Override
			protected QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
				return null;
			}
		};
		assertTrue(group.getAllVariables().isEmpty());
		
		registry.shutdown();
	}
}
