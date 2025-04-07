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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * Helper class for emitting a value along with the window information from a {@link
 * org.apache.flink.streaming.api.functions.windowing.WindowFunction}.
 */
public class WindowedValue<T, W extends Window> {
    private final T value;
    private final W window;

    public WindowedValue(T value, W window) {
        this.value = value;
        this.window = window;
    }

    public T value() {
        return value;
    }

    public W window() {
        return window;
    }

    @Override
    public String toString() {
        return "WindowedValue(" + value + ", " + window + ")";
    }
}
