/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.testutils;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.util.ExternalResource;

/**
 * A JUnit rule that encapsulates {@link InMemoryReporter} properly with test cases.
 *
 * <pre>
 * public static class Test {
 *     &#064;ClassRule
 *     public MiniClusterResource miniCluster = new MiniClusterResource(...);
 *     // ensures that job metrics from different tests don't interfere with each other
 *     &#064;Rule
 *     public InMemoryReporterRule inMemoryReporter = InMemoryReporterRule.create();
 *
 *     &#064;Test
 *     public void test() {
 *         // all task/job + cluster metrics
 *         inMemoryReporter.getReporter().getMetricsByIdentifiers()
 *     }
 * }
 * </pre>
 */
@Experimental
public class InMemoryReporterRule implements ExternalResource {
    private final InMemoryReporter inMemoryReporter = InMemoryReporter.getInstance();

    public static InMemoryReporterRule create() {
        return new InMemoryReporterRule();
    }

    private InMemoryReporterRule() {}

    public InMemoryReporter getReporter() {
        return inMemoryReporter;
    }

    @Override
    public void before() {
        // FLINK-23785: some metrics may be deregistered even after the test case.
        inMemoryReporter.applyRemovals();
    }

    @Override
    public void afterTestSuccess() {
        inMemoryReporter.applyRemovals();
    }
}
