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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests for the {@link JobManagerJobMetricGroup}. */
public class JobManagerJobGroupTest extends TestLogger {

    @Test
    public void testGenerateScopeDefault() throws Exception {
        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryConfiguration.defaultMetricRegistryConfiguration());

        JobManagerMetricGroup tmGroup = new JobManagerMetricGroup(registry, "theHostName");
        JobMetricGroup jmGroup =
                new JobManagerJobMetricGroup(registry, tmGroup, new JobID(), "myJobName");

        assertArrayEquals(
                new String[] {"theHostName", "jobmanager", "myJobName"},
                jmGroup.getScopeComponents());

        assertEquals("theHostName.jobmanager.myJobName.name", jmGroup.getMetricIdentifier("name"));

        registry.shutdown().get();
    }

    @Test
    public void testGenerateScopeCustom() throws Exception {
        Configuration cfg = new Configuration();
        cfg.setString(MetricOptions.SCOPE_NAMING_JM, "abc");
        cfg.setString(MetricOptions.SCOPE_NAMING_JM_JOB, "some-constant.<job_name>");
        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(cfg));

        JobID jid = new JobID();

        JobManagerMetricGroup tmGroup = new JobManagerMetricGroup(registry, "theHostName");
        JobMetricGroup jmGroup = new JobManagerJobMetricGroup(registry, tmGroup, jid, "myJobName");

        assertArrayEquals(
                new String[] {"some-constant", "myJobName"}, jmGroup.getScopeComponents());

        assertEquals("some-constant.myJobName.name", jmGroup.getMetricIdentifier("name"));

        registry.shutdown().get();
    }

    @Test
    public void testGenerateScopeCustomWildcard() throws Exception {
        Configuration cfg = new Configuration();
        cfg.setString(MetricOptions.SCOPE_NAMING_JM, "peter");
        cfg.setString(MetricOptions.SCOPE_NAMING_JM_JOB, "*.some-constant.<job_id>");
        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(cfg));

        JobID jid = new JobID();

        JobManagerMetricGroup tmGroup = new JobManagerMetricGroup(registry, "theHostName");
        JobMetricGroup jmGroup = new JobManagerJobMetricGroup(registry, tmGroup, jid, "myJobName");

        assertArrayEquals(
                new String[] {"peter", "some-constant", jid.toString()},
                jmGroup.getScopeComponents());

        assertEquals("peter.some-constant." + jid + ".name", jmGroup.getMetricIdentifier("name"));

        registry.shutdown().get();
    }

    @Test
    public void testCreateQueryServiceMetricInfo() {
        JobID jid = new JobID();
        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryConfiguration.defaultMetricRegistryConfiguration());
        JobManagerMetricGroup jm = new JobManagerMetricGroup(registry, "host");
        JobManagerJobMetricGroup jmj = new JobManagerJobMetricGroup(registry, jm, jid, "jobname");

        QueryScopeInfo.JobQueryScopeInfo info =
                jmj.createQueryServiceMetricInfo(new DummyCharacterFilter());
        assertEquals("", info.scope);
        assertEquals(jid.toString(), info.jobID);
    }
}
