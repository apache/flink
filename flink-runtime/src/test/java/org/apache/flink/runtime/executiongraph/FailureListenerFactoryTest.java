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
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.factories.UnregisteredJobManagerJobMetricGroupFactory;

import org.junit.Test;

import java.net.URISyntaxException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link org.apache.flink.runtime.executiongraph.DefaultFailureListener} created by {@link
 * org.apache.flink.runtime.executiongraph.FailureListenerFactory}.
 */
public class FailureListenerFactoryTest {

    @Test
    public void testLoadDefaultFailureListener() throws URISyntaxException {
        FailureListenerFactory failureListenerFactory =
                new FailureListenerFactory(new Configuration());

        List<FailureListener> failureListenerList =
                failureListenerFactory.createFailureListener(
                        UnregisteredJobManagerJobMetricGroupFactory.INSTANCE.create(
                                new JobGraph("Test")));

        assertEquals(1, failureListenerList.size());
        assertTrue(failureListenerList.get(0) instanceof DefaultFailureListener);
    }
}
