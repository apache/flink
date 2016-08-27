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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class JobManagerJobGroupTest {

	@Test
	public void testGenerateScopeDefault() {
		MetricRegistry registry = new MetricRegistry(new Configuration());

		JobManagerMetricGroup tmGroup = new JobManagerMetricGroup(registry, "theHostName");
		JobMetricGroup jmGroup = new JobManagerJobMetricGroup(registry, tmGroup, new JobID(), "myJobName");

		assertArrayEquals(
				new String[] { "theHostName", "jobmanager", "myJobName"},
				jmGroup.getScopeComponents());

		assertEquals(
				"theHostName.jobmanager.myJobName.name",
				jmGroup.getMetricIdentifier("name"));

		registry.shutdown();
	}

	@Test
	public void testGenerateScopeCustom() {
		Configuration cfg = new Configuration();
		cfg.setString(ConfigConstants.METRICS_SCOPE_NAMING_JM, "abc");
		cfg.setString(ConfigConstants.METRICS_SCOPE_NAMING_JM_JOB, "some-constant.<job_name>");
		MetricRegistry registry = new MetricRegistry(cfg);

		JobID jid = new JobID();

		JobManagerMetricGroup tmGroup = new JobManagerMetricGroup(registry, "theHostName");
		JobMetricGroup jmGroup = new JobManagerJobMetricGroup(registry, tmGroup, jid, "myJobName");

		assertArrayEquals(
				new String[] { "some-constant", "myJobName" },
				jmGroup.getScopeComponents());

		assertEquals(
				"some-constant.myJobName.name",
				jmGroup.getMetricIdentifier("name"));

		registry.shutdown();
	}

	@Test
	public void testGenerateScopeCustomWildcard() {
		Configuration cfg = new Configuration();
		cfg.setString(ConfigConstants.METRICS_SCOPE_NAMING_JM, "peter");
		cfg.setString(ConfigConstants.METRICS_SCOPE_NAMING_JM_JOB, "*.some-constant.<job_id>");
		MetricRegistry registry = new MetricRegistry(cfg);

		JobID jid = new JobID();

		JobManagerMetricGroup tmGroup = new JobManagerMetricGroup(registry, "theHostName");
		JobMetricGroup jmGroup = new JobManagerJobMetricGroup(registry, tmGroup, jid, "myJobName");

		assertArrayEquals(
				new String[] { "peter", "some-constant", jid.toString() },
				jmGroup.getScopeComponents());

		assertEquals(
				"peter.some-constant." + jid + ".name",
				jmGroup.getMetricIdentifier("name"));

		registry.shutdown();
	}
}
