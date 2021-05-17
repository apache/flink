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

package org.apache.flink.state.api.utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Collection;

/** A wrapper class that allows threads to block until the inner window assigner starts. */
public class WaitingWindowAssigner<T extends Window> extends WindowAssigner<Object, T>
        implements WaitingFunction {
    private final OperatorLatch startLatch = new OperatorLatch();

    private final WindowAssigner<Object, T> delegate;

    private WaitingWindowAssigner(WindowAssigner<Object, T> delegate) {
        this.delegate = delegate;
    }

    public static <T extends Window> WaitingWindowAssigner<T> wrap(
            WindowAssigner<Object, T> delegate) {
        return new WaitingWindowAssigner<>(delegate);
    }

    @Override
    public Collection<T> assignWindows(
            Object element, long timestamp, WindowAssignerContext context) {
        startLatch.trigger();
        return delegate.assignWindows(element, timestamp, context);
    }

    @Override
    public Trigger<Object, T> getDefaultTrigger(StreamExecutionEnvironment env) {
        return delegate.getDefaultTrigger(env);
    }

    @Override
    public TypeSerializer<T> getWindowSerializer(ExecutionConfig executionConfig) {
        return delegate.getWindowSerializer(executionConfig);
    }

    @Override
    public boolean isEventTime() {
        return delegate.isEventTime();
    }

    /** This method blocks until the inner window assigner has started. */
    public void await() throws RuntimeException {
        startLatch.await();
    }
}
