/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests for {@link GlobalWindows}. */
class GlobalWindowsTest {

    @Test
    void testWindowAssignment() {
        WindowAssigner.WindowAssignerContext mockContext =
                mock(WindowAssigner.WindowAssignerContext.class);

        GlobalWindows assigner = GlobalWindows.create();

        assertThat(assigner.assignWindows("String", 0L, mockContext))
                .containsExactly(GlobalWindow.get());
        assertThat(assigner.assignWindows("String", 4999L, mockContext))
                .containsExactly(GlobalWindow.get());
        assertThat(assigner.assignWindows("String", 5000L, mockContext))
                .containsExactly(GlobalWindow.get());
    }

    @Test
    void testProperties() {
        GlobalWindows assigner = GlobalWindows.create();

        assertThat(assigner.isEventTime()).isFalse();
        assertThat(assigner.getWindowSerializer(new ExecutionConfig()))
                .isEqualTo(new GlobalWindow.Serializer());
        assertThat(assigner.getDefaultTrigger()).isInstanceOf(GlobalWindows.NeverTrigger.class);
        assigner = GlobalWindows.createWithEndOfStreamTrigger();
        assertThat(assigner.getDefaultTrigger())
                .isInstanceOf(GlobalWindows.EndOfStreamTrigger.class);
    }
}
