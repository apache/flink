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

package org.apache.flink.runtime.operators.lifecycle;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher;
import org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue;

import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Test {@link JobGraph} with description to allow control its execution and validation. */
public class TestJobWithDescription {
    public final JobGraph jobGraph;
    public final Set<String> sources;
    public final Set<String> operatorsWithLifecycleTracking;
    public final Set<String> operatorsWithDataFlowTracking;
    public final Map<String, Integer> operatorsNumberOfInputs;
    public final TestEventQueue eventQueue;
    public final TestCommandDispatcher commandQueue;

    public TestJobWithDescription(
            JobGraph jobGraph,
            Set<String> sources,
            Set<String> operatorsWithLifecycleTracking,
            Set<String> operatorsWithDataFlowTracking,
            Map<String, Integer> operatorsNumberOfInputs,
            TestEventQueue eventQueue,
            TestCommandDispatcher commandQueue) {
        this.jobGraph = jobGraph;
        this.sources = unmodifiableSet(sources);
        this.operatorsWithLifecycleTracking = unmodifiableSet(operatorsWithLifecycleTracking);
        this.operatorsWithDataFlowTracking = unmodifiableSet(operatorsWithDataFlowTracking);
        this.operatorsNumberOfInputs = unmodifiableMap(operatorsNumberOfInputs);
        this.eventQueue = checkNotNull(eventQueue);
        this.commandQueue = checkNotNull(commandQueue);
    }
}
