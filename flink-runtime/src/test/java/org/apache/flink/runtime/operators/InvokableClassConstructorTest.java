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

package org.apache.flink.runtime.operators;

import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.iterative.task.IterationHeadTask;
import org.apache.flink.runtime.iterative.task.IterationIntermediateTask;
import org.apache.flink.runtime.iterative.task.IterationSynchronizationSinkTask;
import org.apache.flink.runtime.iterative.task.IterationTailTask;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that validate that stateless/stateful task implementations have the corresponding
 * constructors.
 */
class InvokableClassConstructorTest {

    private static final Class<?>[] STATELESS_TASKS = {
        IterationHeadTask.class,
        IterationIntermediateTask.class,
        IterationTailTask.class,
        IterationSynchronizationSinkTask.class,
        DataSourceTask.class,
        DataSinkTask.class
    };

    // ------------------------------------------------------------------------
    //  Tests
    // ------------------------------------------------------------------------

    @Test
    void testNoStatefulConstructor() throws Exception {
        for (Class<?> clazz : STATELESS_TASKS) {

            // check that there is a constructor for Environment only
            clazz.getConstructor(Environment.class);

            // check that there is NO constructor for Environment and Task State
            assertThatThrownBy(
                            () ->
                                    clazz.getDeclaredConstructor(
                                            Environment.class, TaskStateSnapshot.class))
                    .isInstanceOf(NoSuchMethodException.class);
        }
    }
}
