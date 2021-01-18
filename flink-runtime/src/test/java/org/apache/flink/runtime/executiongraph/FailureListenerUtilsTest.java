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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.failurelistener.FailureListener;
import org.apache.flink.runtime.failurelistener.DefaultFailureListener;
import org.apache.flink.runtime.failurelistener.FailureListenerUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.factories.UnregisteredJobManagerJobMetricGroupFactory;

import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link DefaultFailureListener} created by {@link
 * org.apache.flink.runtime.failurelistener.FailureListenerUtils}.
 */
public class FailureListenerUtilsTest {

    @Test
    public void testLoadDefaultFailureListener() {
        final JobGraph jg = new JobGraph("Test");
        Set<FailureListener> failureListeners =
                FailureListenerUtils.getFailureListeners(
                        new Configuration(),
                        jg.getJobID(),
                        jg.getName(),
                        UnregisteredJobManagerJobMetricGroupFactory.INSTANCE.create(jg));

        assertEquals(1, failureListeners.size());
        assertTrue(failureListeners.iterator().next() instanceof DefaultFailureListener);
    }
}
