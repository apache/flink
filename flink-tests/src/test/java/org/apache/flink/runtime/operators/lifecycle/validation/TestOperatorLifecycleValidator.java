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

package org.apache.flink.runtime.operators.lifecycle.validation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.lifecycle.TestJobWithDescription;
import org.apache.flink.runtime.operators.lifecycle.event.TestEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Validates operator lifecycle based on collected {@link TestEvent}s. */
public interface TestOperatorLifecycleValidator {

    void validateOperatorLifecycle(
            TestJobWithDescription job,
            String operatorId,
            int subtaskIndex,
            List<TestEvent> operatorEvents);

    static void checkOperatorsLifecycle(
            TestJobWithDescription testJob, TestOperatorLifecycleValidator... validators) {
        Map<Tuple2<String, Integer>, List<TestEvent>> eventsByOperator = new HashMap<>();
        for (TestEvent ev : testJob.eventQueue.getAll()) {
            eventsByOperator
                    .computeIfAbsent(
                            Tuple2.of(ev.operatorId, ev.subtaskIndex), ign -> new ArrayList<>())
                    .add(ev);
        }
        eventsByOperator.forEach(
                (operatorIdAndIndex, operatorEvents) -> {
                    String id = operatorIdAndIndex.f0;
                    if (testJob.operatorsWithLifecycleTracking.contains(id)) {
                        for (TestOperatorLifecycleValidator validator : validators) {
                            validator.validateOperatorLifecycle(
                                    testJob, id, operatorIdAndIndex.f1, operatorEvents);
                        }
                    }
                });
    }
}
