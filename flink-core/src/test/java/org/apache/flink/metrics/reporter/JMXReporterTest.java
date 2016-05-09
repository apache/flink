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
package org.apache.flink.metrics.reporter;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class JMXReporterTest {
	/**
	 * Verifies that the JMXReporter properly generates the JMX name.
	 */
	@Test
	public void testGenerateName() {
		String name = "metric";

		List<String> scope = new ArrayList<>();
		scope.add("value0");
		scope.add("value1");
		scope.add("\"value2 (test),=;:?'");

		String jmxName = new JMXReporter().generateName(name, scope);

		Assert.assertEquals("org.apache.flink.metrics:key0=value0,key1=value1,key2=value2_(test)------,name=metric", jmxName);
	}
}
