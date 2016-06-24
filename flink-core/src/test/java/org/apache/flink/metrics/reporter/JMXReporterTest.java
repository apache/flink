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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricRegistry;
import org.apache.flink.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.metrics.util.TestReporter;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import static org.apache.flink.metrics.MetricRegistry.KEY_METRICS_REPORTER_CLASS;
import static org.junit.Assert.assertEquals;

public class JMXReporterTest {

	@Test
	public void testReplaceInvalidChars() {
		assertEquals("", JMXReporter.replaceInvalidChars(""));
		assertEquals("abc", JMXReporter.replaceInvalidChars("abc"));
		assertEquals("abc", JMXReporter.replaceInvalidChars("abc\""));
		assertEquals("abc", JMXReporter.replaceInvalidChars("\"abc"));
		assertEquals("abc", JMXReporter.replaceInvalidChars("\"abc\""));
		assertEquals("abc", JMXReporter.replaceInvalidChars("\"a\"b\"c\""));
		assertEquals("", JMXReporter.replaceInvalidChars("\"\"\"\""));
		assertEquals("____", JMXReporter.replaceInvalidChars("    "));
		assertEquals("ab_-(c)-", JMXReporter.replaceInvalidChars("\"ab ;(c)'"));
		assertEquals("a_b_c", JMXReporter.replaceInvalidChars("a b c"));
		assertEquals("a_b_c_", JMXReporter.replaceInvalidChars("a b c "));
		assertEquals("a-b-c-", JMXReporter.replaceInvalidChars("a;b'c*"));
		assertEquals("a------b------c", JMXReporter.replaceInvalidChars("a,=;:?'b,=;:?'c"));
	}

	/**
	 * Verifies that the JMXReporter properly generates the JMX name.
	 */
	@Test
	public void testGenerateName() {
		String[] scope = { "value0", "value1", "\"value2 (test),=;:?'" };
		String jmxName = JMXReporter.generateJmxName("TestMetric", scope);

		assertEquals("org.apache.flink.metrics:key0=value0,key1=value1,key2=value2_(test)------,name=TestMetric", jmxName);
	}

	/**
	 * Verifies that multiple JMXReporters can be started on the same machine and register metrics at the MBeanServer.
     */
	@Test
	public void testPortConflictHandling() throws Exception {
		Configuration cfg = new Configuration();
		cfg.setString(KEY_METRICS_REPORTER_CLASS, TestReporter.class.getName());
		MetricRegistry reg = new MetricRegistry(cfg);

		TaskManagerMetricGroup mg = new TaskManagerMetricGroup(reg, "host", "tm");

		JMXReporter rep1 = new JMXReporter();
		JMXReporter rep2 = new JMXReporter();

		Configuration cfg1 = new Configuration();
		Configuration cfg2 = new Configuration();

		rep1.open(cfg1);
		rep2.open(cfg2);

		rep1.notifyOfAddedMetric(new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return 1;
			}
		}, "rep1", new TaskManagerMetricGroup(reg, "host", "tm"));

		rep2.notifyOfAddedMetric(new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return 2;
			}
		}, "rep2", new TaskManagerMetricGroup(reg, "host", "tm"));

		MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

		ObjectName objectName1 = new ObjectName(JMXReporter.generateJmxName("rep1", mg.getScopeComponents()));
		ObjectName objectName2 = new ObjectName(JMXReporter.generateJmxName("rep2", mg.getScopeComponents()));

		assertEquals(1, mBeanServer.getAttribute(objectName1, "Value"));
		assertEquals(2, mBeanServer.getAttribute(objectName2, "Value"));
		
		rep1.close();
		rep2.close();
		reg.shutdown();
	}
}
